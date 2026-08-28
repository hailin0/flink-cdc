---
title: "DuckLake"
weight: 8
type: docs
aliases:
- /connectors/pipeline-connectors/ducklake
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# DuckLake Pipeline Connector

The DuckLake Pipeline Connector functions as a *Data Sink* for data pipelines,
enabling CDC data writes to [DuckLake](https://ducklake.select/) tables. This
document explains how to configure and use the connector.

## Key Capabilities

* **High-throughput primary-key table mirror:**
materializes the latest row for each primary key without retaining row-level change history
* **Parallel file writing:**
allows each sink writer to produce independent Parquet data and key files
* **Checkpoint-aligned commits:**
uses Flink checkpoints to atomically commit data files, schema changes, and checkpoint metadata
* **Schema synchronization:**
creates target tables and applies supported source schema changes automatically
* **Multiple catalog and storage providers:**
supports a PostgreSQL catalog with object or shared filesystem storage, plus a DuckDB catalog with local filesystem storage for development

How to create Pipeline
----------------

The pipeline for reading data from MySQL and writing it to DuckLake can be defined as follows.

### PostgreSQL Catalog with S3-Compatible Storage Example

The following production-oriented example uses PostgreSQL as the DuckLake catalog and S3-compatible object storage:

```yaml
source:
  type: mysql
  name: MySQL Source
  hostname: mysql.example.com
  port: 3306
  username: flink_cdc
  password: ${MYSQL_PASSWORD}
  tables: inventory.\.*
  server-id: 5400-5404
  server-time-zone: UTC

sink:
  type: ducklake
  name: DuckLake Sink
  catalog.properties.type: postgres
  catalog.properties.host: postgres.example.com
  catalog.properties.port: 5432
  catalog.properties.database: ducklake_catalog
  catalog.properties.user: ducklake
  catalog.properties.password: ${DUCKLAKE_CATALOG_PASSWORD}
  catalog.properties.ssl-mode: require
  storage.properties.type: s3
  storage.properties.path: s3://warehouse/ducklake
  storage.properties.endpoint: https://s3.example.com
  storage.properties.region: us-east-1
  storage.properties.access-key: ${S3_ACCESS_KEY}
  storage.properties.secret-key: ${S3_SECRET_KEY}
  storage.properties.path-style-access: true

pipeline:
  name: MySQL to DuckLake
  schema.change.behavior: evolve
  parallelism: 2
  local-time-zone: UTC
```

The `${...}` placeholders are examples. Supply secrets through the secret management mechanism of your deployment and do not commit them to source control.

### DuckDB Catalog with Local Filesystem Example

For local development, use an absolute DuckDB catalog file and data directory:

```yaml
sink:
  type: ducklake
  name: Local DuckLake Sink
  catalog.properties.type: duckdb
  catalog.properties.path: /tmp/flink-cdc-ducklake/catalog.ducklake
  storage.properties.type: filesystem
  storage.properties.path: /tmp/flink-cdc-ducklake/warehouse
```

This configuration is intended only for tests, demonstrations, and debugging. It is not supported for distributed production jobs.

Supported Deployment
----------------

The built-in catalog providers are `postgres` for a shared production catalog
(PostgreSQL 12 or later) and `duckdb` for a single-process development catalog.
A PostgreSQL catalog can use any built-in storage provider. A DuckDB catalog is
supported only with `filesystem` storage. The built-in storage providers are
summarized below.

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
  <thead>
    <tr>
      <th class="text-left">Storage Type</th>
      <th class="text-left">Path</th>
      <th class="text-left">Credentials and Notes</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>s3</td>
      <td><code>s3://bucket/prefix</code></td>
      <td>Static keys or the AWS credential chain. Configure <code>endpoint</code> and usually <code>path-style-access: true</code> for S3-compatible storage such as MinIO.</td>
    </tr>
    <tr>
      <td>gcs</td>
      <td><code>gcs://bucket/prefix</code> or <code>gs://bucket/prefix</code></td>
      <td>GCS interoperability HMAC access and secret keys. Service-account JSON keys are not supported.</td>
    </tr>
    <tr>
      <td>r2</td>
      <td><code>r2://bucket/prefix</code></td>
      <td>Cloudflare account ID, access key, and secret key.</td>
    </tr>
    <tr>
      <td>azure</td>
      <td><code>az://...</code>, <code>azure://...</code>, or <code>abfss://...</code></td>
      <td>Connection string, Azure credential chain, or service principal.</td>
    </tr>
    <tr>
      <td>filesystem</td>
      <td>Absolute filesystem path</td>
      <td>No credentials. With PostgreSQL, the path must be a genuinely shared filesystem and <code>storage.properties.shared</code> must be <code>true</code>.</td>
    </tr>
  </tbody>
</table>
</div>

`catalog.properties.type=duckdb` is supported only with
`storage.properties.type=filesystem`, and this combination is development-only.
A DuckDB catalog file is single-client and cannot coordinate different
TaskManager processes.

`catalog.properties.type=postgres` with `storage.properties.type=filesystem`
requires a genuinely shared filesystem such as NFS. The same absolute path
must be mounted with identical semantics on every JobManager and TaskManager.
A node-local path is unsafe.

All Flink processes must be able to reach the selected catalog, storage, and
DuckDB extension repository unless the required extensions are pre-installed.

***Note:***
Download `flink-cdc-pipeline-connector-ducklake` and pass the connector JAR with
the `--jar` argument of Flink CDC CLI, or place it in the Flink CDC connector
library directory. The connector JAR already contains DuckDB JDBC. Do not add a
different DuckDB JDBC version to the same classpath.

DuckDB Extension Management
----------------

The connector uses DuckDB JDBC `1.5.5.1` and always loads the `ducklake`
extension for catalog operations. It loads `postgres` for a PostgreSQL catalog,
`httpfs` for S3, GCS, and R2, `azure` for Azure storage, and `aws` for the S3
credential chain. Filesystem and DuckDB catalog providers require no additional
provider extension.

By default, DuckDB installs missing extensions from its official extension
repository at runtime. For a network-restricted cluster, pre-install matching
extensions on every node and set `duckdb.extension-directory`, or expose an
internal mirror through `duckdb.extension-repository`. Extensions must match
the DuckDB version and operating-system platform.

Pipeline Connector Options
----------------

<div class="highlight">
<table class="colwidths-auto docutils">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Option</th>
      <th class="text-left" style="width: 12%">Required</th>
      <th class="text-left" style="width: 12%">Default</th>
      <th class="text-left" style="width: 9%">Type</th>
      <th class="text-left" style="width: 42%">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr><td>type</td><td>required</td><td>(none)</td><td>String</td><td>Connector identifier. Must be <code>ducklake</code>.</td></tr>
    <tr><td>name</td><td>optional</td><td>(none)</td><td>String</td><td>A descriptive name for the sink.</td></tr>
    <tr><td>catalog.properties.type</td><td>required</td><td>(none)</td><td>String</td><td>Catalog provider. Built-in values are <code>postgres</code> and <code>duckdb</code>.</td></tr>
    <tr><td>catalog.properties.path</td><td>conditionally required</td><td>(none)</td><td>String</td><td>Absolute DuckDB catalog file path. Required when <code>catalog.properties.type</code> is <code>duckdb</code>; development use only.</td></tr>
    <tr><td>catalog.properties.host</td><td>conditionally required</td><td>(none)</td><td>String</td><td>PostgreSQL catalog host. Required when <code>catalog.properties.type</code> is <code>postgres</code>.</td></tr>
    <tr><td>catalog.properties.port</td><td>optional</td><td>5432</td><td>Integer</td><td>PostgreSQL catalog port.</td></tr>
    <tr><td>catalog.properties.database</td><td>conditionally required</td><td>(none)</td><td>String</td><td>PostgreSQL database that stores DuckLake metadata. Required for a PostgreSQL catalog.</td></tr>
    <tr><td>catalog.properties.user</td><td>conditionally required</td><td>(none)</td><td>String</td><td>PostgreSQL user. Required for a PostgreSQL catalog.</td></tr>
    <tr><td>catalog.properties.password</td><td>conditionally required</td><td>(none)</td><td>String</td><td>PostgreSQL password. Required for a PostgreSQL catalog.</td></tr>
    <tr><td>catalog.properties.ssl-mode</td><td>optional</td><td>prefer</td><td>String</td><td>PostgreSQL <code>sslmode</code> value passed to DuckLake.</td></tr>
    <tr><td>storage.properties.type</td><td>required</td><td>(none)</td><td>String</td><td>Storage provider. Built-in values are <code>s3</code>, <code>gcs</code>, <code>r2</code>, <code>azure</code>, and <code>filesystem</code>.</td></tr>
    <tr><td>storage.properties.path</td><td>required</td><td>(none)</td><td>String</td><td>DuckLake data root. The scheme must match the storage provider; <code>filesystem</code> requires an absolute path.</td></tr>
    <tr><td>storage.properties.shared</td><td>optional</td><td>false</td><td>Boolean</td><td>Confirms that a filesystem path is identically shared by all Flink processes. Required with a non-DuckDB catalog.</td></tr>
    <tr><td>storage.properties.endpoint</td><td>optional</td><td>(none)</td><td>String</td><td>S3-compatible endpoint. Include <code>http://</code> to disable TLS; an endpoint without a scheme uses TLS.</td></tr>
    <tr><td>storage.properties.region</td><td>optional</td><td>us-east-1 for config</td><td>String</td><td>S3 region. The credential chain may resolve it when omitted.</td></tr>
    <tr><td>storage.properties.credential-provider</td><td>optional</td><td>config</td><td>String</td><td>Credential provider. S3 supports <code>config</code> and <code>credential-chain</code>; Azure also supports <code>service-principal</code>.</td></tr>
    <tr><td>storage.properties.access-key</td><td>conditionally required</td><td>(none)</td><td>String</td><td>Static access key. Required by S3 <code>config</code>, GCS, and R2.</td></tr>
    <tr><td>storage.properties.secret-key</td><td>conditionally required</td><td>(none)</td><td>String</td><td>Static secret key. Required by S3 <code>config</code>, GCS, and R2.</td></tr>
    <tr><td>storage.properties.session-token</td><td>optional</td><td>(none)</td><td>String</td><td>Optional static S3 session token.</td></tr>
    <tr><td>storage.properties.credential-chain</td><td>optional</td><td>DuckDB default</td><td>String</td><td>Semicolon-separated provider chain used by S3 or Azure <code>credential-chain</code>.</td></tr>
    <tr><td>storage.properties.profile</td><td>optional</td><td>AWS default</td><td>String</td><td>AWS profile used by <code>credential-chain</code>.</td></tr>
    <tr><td>storage.properties.path-style-access</td><td>optional</td><td>false</td><td>Boolean</td><td>Whether to use path-style S3 URLs, commonly required by S3-compatible storage.</td></tr>
    <tr><td>storage.properties.account-id</td><td>conditionally required</td><td>(none)</td><td>String</td><td>Cloudflare account ID. Required by R2.</td></tr>
    <tr><td>storage.properties.connection-string</td><td>conditionally required</td><td>(none)</td><td>String</td><td>Azure connection string. Required by the Azure <code>config</code> provider.</td></tr>
    <tr><td>storage.properties.account-name</td><td>conditionally required</td><td>(none)</td><td>String</td><td>Azure storage account name. Required by <code>credential-chain</code> and <code>service-principal</code>.</td></tr>
    <tr><td>storage.properties.tenant-id</td><td>conditionally required</td><td>(none)</td><td>String</td><td>Azure tenant ID. Required by <code>service-principal</code>.</td></tr>
    <tr><td>storage.properties.client-id</td><td>conditionally required</td><td>(none)</td><td>String</td><td>Azure client ID. Required by <code>service-principal</code>.</td></tr>
    <tr><td>storage.properties.client-secret</td><td>conditionally required</td><td>(none)</td><td>String</td><td>Azure client secret. Required by <code>service-principal</code>.</td></tr>
    <tr><td>duckdb.extension-directory</td><td>optional</td><td>(none)</td><td>String</td><td>Directory containing DuckDB extensions that match the embedded DuckDB JDBC version and platform.</td></tr>
    <tr><td>duckdb.extension-repository</td><td>optional</td><td>(none)</td><td>String</td><td>Custom DuckDB extension repository used by <code>INSTALL ... FROM</code>.</td></tr>
    <tr><td>duckdb.memory-limit</td><td>optional</td><td>512MB</td><td>String</td><td>Memory limit for each independently opened embedded DuckDB client. Local DuckDB catalog leases share one client and one limit.</td></tr>
    <tr><td>duckdb.temp-directory</td><td>optional</td><td>JVM temp directory</td><td>String</td><td>Root directory for the spill directory of each independently opened DuckDB client. Local catalog leases share that directory.</td></tr>
    <tr><td>sink.commit.max-retries</td><td>optional</td><td>8</td><td>Integer</td><td>Maximum retries for retryable catalog conflicts. Must not be negative.</td></tr>
    <tr><td>sink.commit.retry-backoff</td><td>optional</td><td>100 ms</td><td>Duration</td><td>Initial exponential retry backoff for catalog conflicts. Must not be negative.</td></tr>
    <tr><td>sink.orphan-cleanup.interval</td><td>optional</td><td>0 ms</td><td>Duration</td><td>Interval for best-effort orphan cleanup; zero disables automatic cleanup.</td></tr>
    <tr><td>sink.orphan-cleanup.retention</td><td>optional</td><td>7 d</td><td>Duration</td><td>Minimum age of an unregistered file before cleanup. Must exceed the maximum suspended-job recovery window.</td></tr>
    <tr><td>sink.writer.max-events-per-file</td><td>optional</td><td>100000</td><td>Long</td><td>Maximum CDC events buffered for one table by one writer before rolling files.</td></tr>
    <tr><td>sink.writer.max-buffered-events</td><td>optional</td><td>1000000</td><td>Long</td><td>Maximum CDC events buffered across all tables by one writer. The least recently used table buffer is rolled at the limit.</td></tr>
    <tr><td>sink.writer.max-open-tables</td><td>optional</td><td>128</td><td>Integer</td><td>Maximum open table buffers per writer. The least recently used buffer is rolled before opening another one.</td></tr>
    <tr><td>sink.id-prefix</td><td>optional</td><td>flink-cdc-</td><td>String</td><td>Prefix used to derive the stable writer identity stored in checkpoint state and commit markers.</td></tr>
  </tbody>
</table>
</div>

Usage Notes
--------

* Every target table must define at least one primary-key column.

* Primary-key columns must be `NOT NULL`.

* An `UPDATE` that changes a primary-key value is rejected. Represent such a change as a delete followed by an insert at the source.

* Each writer creates files only for the rows assigned to it and does not update files produced by other writers.

* The connector creates a high-throughput current-state mirror and does not preserve row-level change history.

Data Type Mapping
----------------

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
  <thead>
    <tr>
      <th class="text-left">Flink CDC Type</th>
      <th class="text-left">DuckDB/DuckLake Type</th>
      <th class="text-left" style="width:60%;">Note</th>
    </tr>
  </thead>
  <tbody>
    <tr><td>CHAR, VARCHAR, STRING</td><td>VARCHAR</td><td>Length is not enforced by DuckLake.</td></tr>
    <tr><td>BOOLEAN</td><td>BOOLEAN</td><td></td></tr>
    <tr><td>BINARY, VARBINARY, BYTES</td><td>BLOB</td><td>Length is not enforced by DuckLake.</td></tr>
    <tr><td>DECIMAL(p, s)</td><td>DECIMAL(p, s)</td><td>Maximum precision is 38.</td></tr>
    <tr><td>TINYINT</td><td>TINYINT</td><td></td></tr>
    <tr><td>SMALLINT</td><td>SMALLINT</td><td></td></tr>
    <tr><td>INTEGER</td><td>INTEGER</td><td></td></tr>
    <tr><td>BIGINT</td><td>BIGINT</td><td></td></tr>
    <tr><td>FLOAT</td><td>FLOAT</td><td></td></tr>
    <tr><td>DOUBLE</td><td>DOUBLE</td><td></td></tr>
    <tr><td>DATE</td><td>DATE</td><td></td></tr>
    <tr><td>TIME(p)</td><td>TIME</td><td>Maximum precision is 6.</td></tr>
    <tr><td>TIMESTAMP(0)</td><td>TIMESTAMP_S</td><td></td></tr>
    <tr><td>TIMESTAMP(1..3)</td><td>TIMESTAMP_MS</td><td></td></tr>
    <tr><td>TIMESTAMP(4..6)</td><td>TIMESTAMP</td><td></td></tr>
    <tr><td>TIMESTAMP(7..9)</td><td>TIMESTAMP_NS</td><td></td></tr>
    <tr><td>TIMESTAMP_LTZ(p)</td><td>TIMESTAMPTZ</td><td>Maximum precision is 6; conversion uses <code>local-time-zone</code> in the <code>pipeline</code> section.</td></tr>
  </tbody>
</table>
</div>

`TIMESTAMP WITH TIME ZONE`, `ARRAY`, `MAP`, `ROW`, and `VARIANT` are not
supported. Only physical columns are supported, and column default expressions
are rejected.

Schema Evolution
----------------

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
  <thead>
    <tr>
      <th class="text-left">Schema Change</th>
      <th class="text-left">Support</th>
      <th class="text-left" style="width:60%;">Restrictions</th>
    </tr>
  </thead>
  <tbody>
    <tr><td>Create table</td><td>Supported</td><td>Requires a non-null primary key, physical columns, supported types, and no column defaults.</td></tr>
    <tr><td>Add column</td><td>Supported</td><td>The new column must be nullable, have no default, and be appended at <code>LAST</code>.</td></tr>
    <tr><td>Drop column</td><td>Supported</td><td>A primary-key column cannot be dropped.</td></tr>
    <tr><td>Alter column type</td><td>Supported</td><td>Supports only <code>TINYINT</code> → <code>SMALLINT</code>/<code>INTEGER</code>/<code>BIGINT</code>, <code>SMALLINT</code> → <code>INTEGER</code>/<code>BIGINT</code>, <code>INTEGER</code> → <code>BIGINT</code>, and <code>FLOAT</code> → <code>DOUBLE</code>. Primary-key columns cannot be changed.</td></tr>
    <tr><td>Rename column</td><td>Supported</td><td>Primary-key columns cannot be renamed. Targets must be unique; chained or cyclic renames and reuse of a previously dropped column name are not supported. The connector materializes the renamed column at the end of the table, so this operation changes <code>SELECT *</code> column order and scans and updates the table.</td></tr>
    <tr><td>Rename table</td><td>Not supported</td><td>Route to a new target table instead.</td></tr>
    <tr><td>Truncate/drop table</td><td>Not supported</td><td>Perform lifecycle management outside this connector.</td></tr>
  </tbody>
</table>
</div>

Three-part target table identifiers are not supported. Route a source table to
a two-part `schema.table` target before it reaches this sink.
The schemas `_flink_cdc_internal` and `_flink_cdc_staging` are reserved for
connector metadata and storage staging respectively.

Checkpoint Visibility and File Lifecycle
----------------

Writers flush immutable data files under
`<data-path>/<schema>/<table>/flink-cdc/.../writer-epoch-<n>` and primary-key
staging files under `<data-path>/_flink_cdc_staging/keys/.../writer-epoch-<n>`.
The schema and table path components are percent-encoded. At checkpoint commit,
the committer reads the key files to delete previous rows, registers non-empty
data files in place with DuckLake, and writes a checkpoint marker in the same
catalog transaction. A repeated commit with the same marker and plan hash is a
no-op; a different plan for an existing marker fails.

When a schema event reaches the sink, each writer first flushes data written
with the previous schema, records the schema event as a checkpoint committable,
and then starts a new schema batch. The metadata applier performs synchronous
capability validation only; it does not modify the DuckLake catalog.

For each table, the globally partitioned commit stream applies a checkpoint in
schema-batch order: files written with the previous schema, the schema change,
and then files written with the new schema. All file registrations, row deletes,
schema changes, and the checkpoint marker run through DuckDB JDBC in one DuckLake
transaction. A catalog error while applying DDL therefore fails the checkpoint
commit and is handled by the same retry and recovery path as a data commit failure.

The internal DuckLake schema `_flink_cdc_internal` stores checkpoint markers
and source primary-key metadata. The committer uses this metadata to validate
primary-key-sensitive schema changes after recovery.

There is no staging-to-final file copy. Once registered, the immutable file in
the table directory is a live DuckLake data file owned by DuckLake and must not
be modified or deleted by the connector.

The connector does not delete key files or uncommitted/orphan data files
directly from storage. When `sink.orphan-cleanup.interval` is greater than zero,
the committer periodically calls DuckLake's `ducklake_delete_orphaned_files`
maintenance function. When automatic cleanup is disabled, run the same function
externally. In either case, use a retention period longer than the maximum
checkpoint recovery window. This removes eligible untracked files while
preserving files referenced by the catalog. Do not apply an independent storage
lifecycle rule to table data directories; the staging prefix may only use a
lifecycle policy whose retention is longer than the maximum recovery window.

Data changes, supported DDL, and the checkpoint marker become visible atomically
when the checkpoint transaction commits. Files written before a rename are
registered first. The committer then adds the target column, copies the source
values, restores the target nullability constraint, and drops the source column
before registering files written with the new name. The target column is appended,
so consumers must not rely on `SELECT *` preserving the previous column order.
This materialized rename avoids relying on name mappings across multiple DuckLake
schema versions, but it causes I/O proportional to the existing table size and
may expose full-table updates through DuckLake change feeds. Run large-table
renames during a suitable maintenance window.

{{< top >}}
