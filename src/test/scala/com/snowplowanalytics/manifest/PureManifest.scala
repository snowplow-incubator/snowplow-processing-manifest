package com.snowplowanalytics.manifest

import java.util.UUID
import java.time.Instant

import cats.data.{ State => CState, _ }
import cats.implicits._

import core._
import core.ProcessingManifest._

object PureManifest
  extends ProcessingManifest[EitherT[CState[List[Record], ?], ManifestError, ?]](SpecHelpers.igluEmbeddedResolver)
    with PureManifest {

  type ManifestState[A] = EitherT[CState[List[Record], ?], ManifestError, A]

  def put(itemId: ItemId,
          app: Application,
          parentRecordId: Option[UUID],
          step: core.State,
          author: Option[Agent],
          payload: Option[Payload]): ManifestState[(UUID, Instant)] =
    EitherT.right(putS(itemId, app, parentRecordId, step, author, payload))

  def list: ManifestState[List[Record]] =
    EitherT.right(listS)

  def getItem(id: ProcessingManifest.ItemId): ManifestState[Option[Item]] = {
    val itemS = for {
      i <- getItemS(id)
    } yield i match {
      case Some(ii) => ii.ensure[Either[ManifestError, ?]](resolver).map(_.some)
      case None => none[Item].asRight
    }

    EitherT[CState[List[Record], ?], ManifestError, Option[Item]](itemS)
  }
}

object Foo {
  def t = {
    for {
      _ <- PureManifest.put("foo", Application("bar", "baz"), Some(UUID.randomUUID()), core.State.New, None, None)
      r <- PureManifest.getItem("foo")
    } yield r
  }
  t.value.run(Nil).value
}



trait PureManifest {
  import PureManifest._

  def getItemS(id: ProcessingManifest.ItemId): CState[List[Record], Option[Item]] = {
    CState((records: List[Record]) => {
      val item = records.filter(_.itemId == id) match {
        case Nil => None
        case h :: t => Some(Item(NonEmptyList(h, t)))
      }
      (records, item)
    })
  }

  def putS(itemId: ItemId,
           app: Application,
           parentRecordId: Option[UUID],
           step: core.State,
           author: Option[Agent],
           payload: Option[Payload]): CState[List[Record], (UUID, Instant)] = {

    val a = Author(author.getOrElse(app.agent), ProcessingManifest.Version)
    def f(records: List[Record]) = {
      val id = seed(records.toSet)
      val timestamp = time(records.toSet)
      val newRecord = Record(itemId, app, id, parentRecordId, step, timestamp, a, payload)
      (newRecord :: records, (id, timestamp))
    }
    CState(f)
  }

  def listS: CState[List[Record], List[Record]] = {
    def f(records: List[Record]) = (records, records)
    CState(f)
  }

  val StartTime = Instant.ofEpochMilli(1524870034204L)
  def id(num: Int): UUID = UUID.nameUUIDFromBytes(Array(java.lang.Byte.valueOf(num.toString)))

  def seed(set: Set[_]): UUID = id(set.size)
  def time(set: Set[_]): Instant = StartTime.plusSeconds(set.size)

}
