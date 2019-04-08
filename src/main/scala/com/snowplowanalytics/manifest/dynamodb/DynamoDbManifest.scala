/*
 * Copyright (c) 2018 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.manifest
package dynamodb

import scala.collection.convert.decorateAsScala._
import scala.collection.convert.decorateAsJava._
import scala.language.higherKinds
import scala.util.control.NonFatal

import java.time.Instant
import java.util.UUID

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.{Record => _, Stream => _, _}
import com.amazonaws.services.dynamodbv2.document.Table

import cats._
import cats.data.NonEmptyList
import cats.implicits._
import cats.effect._

import fs2._

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.circe.implicits._

import core._
import core.ProcessingManifest._
import core.ManifestError._

import Common._
import Serialization._

/**
  * AWS DynamoDB implementation of processing manifests
  */
class DynamoDbManifest[F[_]](val client: AmazonDynamoDB,
                             val primaryTable: String,
                             resolver: Resolver[F])
                            (implicit F: ManifestAction[F],
                                      C: Clock[F],
                                      L: RegistryLookup[F])
  extends ProcessingManifest[F](resolver) {
  import DynamoDbManifest._

  /**
    * Turn raw `Record` elements into DynamoDB `DbItem`.
    * Can be overloaded by subclasses
    */
  protected[dynamodb] def createRecord(itemId: ItemId,
                                       app: Application,
                                       recordId: UUID,
                                       previousRecordId: Option[UUID],
                                       state: State,
                                       time: Instant,
                                       author: Author,
                                       payload: Option[Payload]): DbRecord = {

    val dataMap = payload
      .map(dataToAttribute)
      .getOrElse(Map.empty)
    val previousRecordMap = previousRecordId
      .map(uuid => Map(PreviousRecordId -> new AttributeValue().withS(uuid.toString)))
      .getOrElse(Map.empty)
    val instanceIdMap = app
      .instanceId
      .map(id => Map(ApplicationInstanceIdKey -> new AttributeValue().withS(id)))
      .getOrElse(Map.empty)

    val item = Map(
      ItemIdKey             -> new AttributeValue().withS(itemId),
      ApplicationNameKey    -> new AttributeValue().withS(app.name),
      ApplicationVersionKey -> new AttributeValue().withS(app.version),
      RecordIdKey           -> new AttributeValue().withS(recordId.toString),
      StateKey              -> new AttributeValue().withS(state.show),
      TimestampKey          -> new AttributeValue().withN(time.toEpochMilli.toString),
      AuthorKey             -> new AttributeValue().withS(showAuthor(author))
    ) ++ dataMap ++ previousRecordMap ++ instanceIdMap

    item.asJava
  }

  def getItem(id: String): F[Option[Item]] =
    for {
      records <- getItemRecords(id)
      item <-  NonEmptyList.fromList(records).map(Item(_).ensure[F](resolver)).sequence
    } yield item

  def put(itemId: ItemId,
          app: Application,
          previousRecordId: Option[UUID],
          state: State,
          author: Option[Agent],
          payload: Option[Payload]): F[(UUID, Instant)] = {

    val time = Instant.now()
    val recordId = UUID.randomUUID()
    val realAuthor = author match {
      case Some(a) => Author(a, Version)
      case None => Author(app.agent, Version)
    }
    val dbRecord = createRecord(itemId, app, recordId, previousRecordId, state, time, realAuthor, payload)
    addRecord(recordId, time, dbRecord)
  }

  def fetch(processedBy: Option[Application], state: Option[State])
           (implicit F: ManifestAction[F], S: Sync[F]): Stream[F, ItemId] =
    Query.fetch[F](client, primaryTable)(processedBy, state)

  def stream(implicit S: Sync[F]): Stream[F, Record] = {
    val dbRecords = for {
      req <- Stream.eval(getRequest)
      record <- Query.scan[F](client, req)
    } yield record
    dbRecords.map(parse(_).foldF[F]).evalMap(i => i)
  }

  /** Low-level item accessor */
  def getItemRecords(itemId: ItemId): F[List[Record]] = {
    val attributeValues = List(new AttributeValue().withS(itemId)).asJava
    val condition = new Condition()
      .withComparisonOperator(ComparisonOperator.EQ)
      .withAttributeValueList(attributeValues)
    val req = new QueryRequest(primaryTable)
      .withConsistentRead(true)
      .withKeyConditions(Map(ItemIdKey -> condition).asJava)
    val request = Eval.always(req)

    val response = Common.query[F](client, request)

    for {
      result <- response
      records <- result.traverse[F, Record](parse(_).foldF[F])
    } yield records
  }

  /** Delete record from manifest (DynamoDB-specific API) */
  def deleteRecord(record: Record): F[Unit] = {
    try {
      val dbItem = createRecord(record.itemId, record.application, record.recordId, record.previousRecordId, record.state, record.timestamp, record.author, None)
        .asScala
        .filterKeys(k => k == Common.ItemIdKey || k == Common.RecordIdKey).asJava
      val _ = client.deleteItem(primaryTable, dbItem)
      ().pure[F]
    } catch {
      case NonFatal(e) =>
        val message = s"DynamoDB Run manifest error: Cannot delete record ${record.recordId}. ${Option(e.getMessage).getOrElse("no error message")}"
        val error: ManifestError = IoError(message)
        error.raiseError[F, Unit]
    }
  }

  /** Add exact same record to manifest (low-level DynamoDB-specific API) */
  def putRecord(r: Record)(implicit F: Sync[F]): F[(UUID, Instant)] = {
    val dbItem = createRecord(r.itemId, r.application, r.recordId, r.previousRecordId, r.state, r.timestamp, r.author, r.payload)
    F.delay(addRecord(r.recordId, r.timestamp, dbItem)).flatten
  }

  /** Create Scan request (wrapped in `Sync` as non-referentially transparent) */
  private[dynamodb] def getRequest(implicit F: Sync[F]): F[ScanRequest] =
    F.delay(new ScanRequest().withTableName(primaryTable))

  private[manifest] def addRecord(recordId: UUID, time: Instant, dbItem: DbRecord): F[(UUID, Instant)] = {
    val request = new PutItemRequest()
      .withTableName(primaryTable)
      .withConditionExpression(s"attribute_not_exists($ItemIdKey) AND attribute_not_exists($RecordIdKey)")
      .withItem(dbItem)

    try {
      val _ = client.putItem(request)
      (recordId, time).pure[F]
    } catch {
      case _: ConditionalCheckFailedException =>
        parseError(s"Record with id:app:state triple already exists. Cannot write $dbItem").raiseError[F, (UUID, Instant)]
      case NonFatal(e) =>
        val message = s"DynamoDB Run manifest error: ${Option(e.getMessage).getOrElse("no error message")}"
        val error: ManifestError = IoError(message)
        error.raiseError[F, (UUID, Instant)]
    }
  }

  def query(application: Application): F[Set[ItemId]] = {
    val req = notProcessedRequest(application)
    val query = Eval.always(req)
    for {
      records <- Common.query[F](client, query)
      itemIds <- records.traverse[F, ItemId](parseItemId(_).foldF[F])
    } yield itemIds.toSet
  }
}

object DynamoDbManifest {

  val DefaultThroughput = 10L

  def apply[F[_]: ManifestAction: Clock: RegistryLookup](dynamodbClient: AmazonDynamoDB,
                                                         tableName: String,
                                                         resolver: Resolver[F]): DynamoDbManifest[F] =
    new DynamoDbManifest[F](dynamodbClient, tableName, resolver)

  /** Helper method to create manifest-compatible DynamoDB table */
  def create[F[_]: ManifestAction](client: AmazonDynamoDB, tableName: String): F[Unit] = {
    val pks = List(
      new AttributeDefinition(ItemIdKey, ScalarAttributeType.S),
      new AttributeDefinition(RecordIdKey, ScalarAttributeType.S))
    val schema = List(
      new KeySchemaElement(ItemIdKey, KeyType.HASH),
      new KeySchemaElement(RecordIdKey, KeyType.RANGE))

    val request = new CreateTableRequest()
      .withTableName(tableName)
      .withAttributeDefinitions(pks.asJava)
      .withKeySchema(schema.asJava)
      .withProvisionedThroughput(new ProvisionedThroughput(DefaultThroughput, DefaultThroughput))

    for {
      response <- ManifestIO[F, CreateTableResult](client.createTable(request))
      table    <- ManifestIO[F, Table](new Table(client, tableName, response.getTableDescription))
      _        <- ManifestIO[F, Unit] { table.waitForActive(); () }     // Block until table is available
    } yield ()
  }

  /** Turn `data` (HashMap of JSONs) into DynamoDB-ready Attribute */
  private[dynamodb] def dataToAttribute(payload: Payload): Map[String, AttributeValue] = {
    val attributeValue = new AttributeValue().withS(payload.normalize.noSpaces)
    Map(DataKey -> attributeValue)
  }
}
