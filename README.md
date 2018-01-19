# Snowplow Processing Manifest

[![Build Status][travis-image]][travis]  [![Release][release-image]][releases] [![License][license-image]][license]

Snowplow Processing Manifest is abstract interface and several implementations of data-processing manifest, keeping track of state of data lake.
Highly inspired by event sourcing and idea of immutable logs.

## Concepts

### Item

Item is precisely defined volume of data. In a simplest and most common case it is a directory on a blob storage.
Item usually bears two pieces of information: 
1. state, which consists of one or more *Records*
2. *id*, to uniquely identify a set of data (such as URL, file path or timedelta)

Example: id - `s3://snowplow-com-acme-processing/shredded/good/run=2017-02-15-13-10-30/`, state - new.

### Record

Record is a smallest, atomic entity of processing manifest.
It represents any *significant* change in Item's state, such as "Item has been discovered", "Item has been locked for processing", "Item processing failed" etc.

Physically each Record represented as row in append-only table. In other words `Record` is the only real entity. `Item` is derived based only on `Record`s it consist of.
Record contains:
1. Item id (so all `Records` can be grouped into single `Item`)
2. Application that added the Record. Usually some step in pipeline that processed an item.
3. State: `New`, `Processing`, `Processed`, `Skipped`, `Failed`, `Resolved`
4. Optionally, payload provided by application

Example: item `s3://snowplow-com-acme-processing/shredded/good/run=2017-02-15-13-10-30/`, state - `Processed`, application - RDB Shredder, payload - shredded type `iglu:com.acme/context/jsonschema/1-0-0` discovered.

### State

As explained below, state is one of following: `New`, `Processing`, `Processed`, `Skipped`, `Failed`, `Resolved`.
Also, *run id* is attached to each state - so single application during single run adds different states with same run id (don't confuse with snowplow run ids - `run=...`)

Each of these states has particular implications, such as:

* `New` - item was just discovered (or added) by some application, so this is a first `Record` that declares existence of an Item.
* `Processing` - some application started to process an Item, effectively meaning "acquire a lock" or "prohibit any other applications to add Records for this Item"
* `Processed` - some application finished to process an Item, effectively meaning "release a lock" or "next application can start processing the Item". Any Item with `Processing`, but without `Processed` considered locked.
* `Failed` - instead of successful `Processed` application ended up with failure. Optionally can contain reason of failure in payload. Item with last `Failed` as last step is considered locked
* `Resolved` - operator was notified about `Failed` state and resolved an issue. Application can add new `Processing`, `Processed` etc
* `Skipped` - forget about Item, no application should take any actions of it

It should be obvious that some types of states are "coupled". E.g. `Processing` should be always coupled with either `Processed` or `Failed`.
It is achieved by records ids - `Processed` record should always refer to `Processing` by its `previousRecordId`.
If it is not coupled with `Processed` - then item is locked and application needs to wait until lock released.
If it is coupled with `Failed` (instead `Processed`) - then item is fail-locked and operator needs to resolve a failure first (by adding `Resolved`).
If record refers to record with wrong state (e.g. `Failed` refers to `Processed` - item is considered invalid, this should never happen unless manifest modified manually).

## Use cases

Primary use case is a *pipeline bus* or *pipeline's source of truth*.
Different components of pipeline such as ETL jobs, storage loaders and data modeling tools can save their state to Processing Manifest.
Subsequent steps in turn can base their behavior on derived state.

This makes Processing Manifest some kind of command post where operator can manage most of aspects of running pipeline from single place,
without being forced to deal with individual components such as blob storage, batch jobs, target database.

For example, whole state of target database (deployed tables, amount of events) can be derived based only on records added by loader.

## Challenges

Snowplow Processing Manifest is considered beta and currently have following limitations:

+ **Single implementation** - Processing Manifest designed to be abstract interface with multiple implementations,
where each implementation provides base guarantees (such as consistency and absence of race conditions), 
plus different performance and transactional characteristics.
Right now only Amazon DynamoDB implementation is available.
+ **Performance** - consequence of previous point. Implementation of many operations in Amazon DynamoDB is far from ideal
and requires global `Scan`, which is alright for small manifests (up to 100.000 `Records`), but can be a bottleneck for bigger ones.
+ **Batch-orientation** - also consequence of previous two points. Current performance characteristics don't allow to write streaming data, e.g. each few seconds. This can be fixed in other upcomming implementations.

## Quickstart


Assuming git, **[Vagrant][vagrant-install]** and **[VirtualBox][virtualbox-install]** installed:

```bash
host$ git clone https://github.com/snowplow-incubator/snowplow-processing-manifest.git
host$ cd snowplow-processing-manifest
host$ vagrant up && vagrant ssh
guest$ cd /vagrant
guest$ sbt test
```

## Copyright and License

Snowplow Processing Manifest is copyright 2018 Snowplow Analytics Ltd.

Licensed under the **[Apache License, Version 2.0][license]** (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


[vagrant-install]: http://docs.vagrantup.com/v2/installation/index.html
[virtualbox-install]: https://www.virtualbox.org/wiki/Downloads

[travis]: https://travis-ci.org/snowplow-incubator/snowplow-processing-manifest
[travis-image]: https://travis-ci.org/snowplow-incubator/snowplow-processing-manifest.png?branch=master

[license]: http://www.apache.org/licenses/LICENSE-2.0
[license-image]: http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat

[release-image]: http://img.shields.io/badge/release-0.1.0-blue.svg?style=flat
[releases]: https://github.com/snowplow-incubator/snowplow-processing-manifest/releases
