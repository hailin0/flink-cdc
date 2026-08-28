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

# DuckLake Pipeline 连接器

DuckLake Pipeline 连接器可以作为数据管道的 *Data Sink*，将 CDC 数据写入
[DuckLake](https://ducklake.select/) 表。本文介绍如何配置及使用该连接器。

## 核心能力

* **高吞吐主键表同步：**
物化每个主键对应的最新行，不保留逐行变更历史
* **并行文件写入：**
每个 Sink Writer 独立生成 Parquet 数据文件和主键文件
* **Checkpoint 驱动提交：**
由 Flink checkpoint 原子提交数据文件、Schema 变更和 checkpoint 元数据
* **Schema 同步：**
自动创建目标表，并同步支持的源表 Schema 变更
* **多种 Catalog 和存储：**
支持 PostgreSQL Catalog 搭配对象存储或共享文件系统，以及仅用于开发的 DuckDB Catalog 搭配本地文件系统

如何创建 Pipeline
----------------

从 MySQL 读取数据并写入 DuckLake 的 Pipeline 可以定义如下。

### PostgreSQL Catalog 与 S3 兼容存储示例

下面的生产配置使用 PostgreSQL 作为 DuckLake Catalog，并使用 S3 兼容对象存储：

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

上面的 `${...}` 是示例占位符。请通过部署环境的密钥管理机制提供密码，不要将密钥提交到源代码仓库。

### DuckDB Catalog 与本地文件系统示例

本地开发可以使用绝对路径形式的 DuckDB Catalog 文件和数据目录：

```yaml
sink:
  type: ducklake
  name: Local DuckLake Sink
  catalog.properties.type: duckdb
  catalog.properties.path: /tmp/flink-cdc-ducklake/catalog.ducklake
  storage.properties.type: filesystem
  storage.properties.path: /tmp/flink-cdc-ducklake/warehouse
```

该配置只适用于测试、演示和调试，不支持分布式生产作业。

支持的部署方式
----------------

内置 Catalog Provider 包括用于共享生产 Catalog 的 `postgres`（PostgreSQL 12
或更高版本），以及用于单进程开发 Catalog 的 `duckdb`。PostgreSQL Catalog 可以
搭配任意内置 Storage Provider；DuckDB Catalog 只支持 `filesystem`。内置 Storage
Provider 如下。

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
  <thead>
    <tr>
      <th class="text-left">Storage 类型</th>
      <th class="text-left">路径</th>
      <th class="text-left">凭证与说明</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>s3</td>
      <td><code>s3://bucket/prefix</code></td>
      <td>静态密钥或 AWS credential chain。MinIO 等 S3 兼容存储需要配置 <code>endpoint</code>，通常还需设置 <code>path-style-access: true</code>。</td>
    </tr>
    <tr>
      <td>gcs</td>
      <td><code>gcs://bucket/prefix</code> 或 <code>gs://bucket/prefix</code></td>
      <td>GCS interoperability HMAC access key 和 secret key，不支持普通 service-account JSON key。</td>
    </tr>
    <tr>
      <td>r2</td>
      <td><code>r2://bucket/prefix</code></td>
      <td>Cloudflare account ID、access key 和 secret key。</td>
    </tr>
    <tr>
      <td>azure</td>
      <td><code>az://...</code>、<code>azure://...</code> 或 <code>abfss://...</code></td>
      <td>Connection string、Azure credential chain 或 service principal。</td>
    </tr>
    <tr>
      <td>filesystem</td>
      <td>文件系统绝对路径</td>
      <td>无需凭证。配合 PostgreSQL 时必须使用真正的共享文件系统，并将 <code>storage.properties.shared</code> 设置为 <code>true</code>。</td>
    </tr>
  </tbody>
</table>
</div>

`catalog.properties.type=duckdb` 只支持
`storage.properties.type=filesystem`，且该组合仅用于开发。DuckDB Catalog 文件是
单客户端模式，无法协调不同的 TaskManager 进程。

`catalog.properties.type=postgres` 与 `storage.properties.type=filesystem`
的组合必须使用 NFS 等真正的共享文件系统。所有 JobManager 和 TaskManager 必须以
相同语义挂载同一个绝对路径，节点本地路径是不安全的。

除非预装扩展，否则所有 Flink 进程都必须能够访问所选 Catalog、存储及 DuckDB
扩展仓库。

***注意：***
下载 `flink-cdc-pipeline-connector-ducklake`，通过 Flink CDC CLI 的 `--jar`
参数传入连接器 JAR，或者将其放入 Flink CDC connector 库目录。连接器 JAR 已包含
DuckDB JDBC，请勿在同一 classpath 中加入其他版本的 DuckDB JDBC。

DuckDB 扩展管理
----------------

连接器使用 DuckDB JDBC `1.5.5.1`，Catalog 操作始终加载 `ducklake` 扩展；
PostgreSQL Catalog 加载 `postgres`，S3、GCS 和 R2 加载 `httpfs`，Azure 加载
`azure`，S3 credential chain 加载 `aws`。文件系统和 DuckDB Catalog Provider
不需要额外的 Provider 扩展。

默认情况下，DuckDB 会在运行时从官方扩展仓库安装缺失扩展。对于网络受限集群，请在
每个节点预装匹配的扩展并配置 `duckdb.extension-directory`，或者通过
`duckdb.extension-repository` 指向内部镜像。扩展必须与 DuckDB 版本和操作系统
平台匹配。

Pipeline Connector 参数
----------------

<div class="highlight">
<table class="colwidths-auto docutils">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">参数</th>
      <th class="text-left" style="width: 12%">是否必填</th>
      <th class="text-left" style="width: 12%">默认值</th>
      <th class="text-left" style="width: 9%">类型</th>
      <th class="text-left" style="width: 42%">说明</th>
    </tr>
  </thead>
  <tbody>
    <tr><td>type</td><td>必填</td><td>(none)</td><td>String</td><td>连接器标识，必须为 <code>ducklake</code>。</td></tr>
    <tr><td>name</td><td>可选</td><td>(none)</td><td>String</td><td>Sink 的描述性名称。</td></tr>
    <tr><td>catalog.properties.type</td><td>必填</td><td>(none)</td><td>String</td><td>Catalog Provider，内置值为 <code>postgres</code> 和 <code>duckdb</code>。</td></tr>
    <tr><td>catalog.properties.path</td><td>条件必填</td><td>(none)</td><td>String</td><td>DuckDB Catalog 文件的绝对路径。<code>catalog.properties.type</code> 为 <code>duckdb</code> 时必填，仅用于开发场景。</td></tr>
    <tr><td>catalog.properties.host</td><td>条件必填</td><td>(none)</td><td>String</td><td>PostgreSQL Catalog 主机。使用 PostgreSQL Catalog 时必填。</td></tr>
    <tr><td>catalog.properties.port</td><td>可选</td><td>5432</td><td>Integer</td><td>PostgreSQL Catalog 端口。</td></tr>
    <tr><td>catalog.properties.database</td><td>条件必填</td><td>(none)</td><td>String</td><td>存储 DuckLake 元数据的 PostgreSQL 数据库。使用 PostgreSQL Catalog 时必填。</td></tr>
    <tr><td>catalog.properties.user</td><td>条件必填</td><td>(none)</td><td>String</td><td>PostgreSQL 用户。使用 PostgreSQL Catalog 时必填。</td></tr>
    <tr><td>catalog.properties.password</td><td>条件必填</td><td>(none)</td><td>String</td><td>PostgreSQL 密码。使用 PostgreSQL Catalog 时必填。</td></tr>
    <tr><td>catalog.properties.ssl-mode</td><td>可选</td><td>prefer</td><td>String</td><td>传给 DuckLake 的 PostgreSQL <code>sslmode</code>。</td></tr>
    <tr><td>storage.properties.type</td><td>必填</td><td>(none)</td><td>String</td><td>Storage Provider，内置值为 <code>s3</code>、<code>gcs</code>、<code>r2</code>、<code>azure</code> 和 <code>filesystem</code>。</td></tr>
    <tr><td>storage.properties.path</td><td>必填</td><td>(none)</td><td>String</td><td>DuckLake 数据根目录。路径 scheme 必须与 Storage Provider 匹配；<code>filesystem</code> 必须使用绝对路径。</td></tr>
    <tr><td>storage.properties.shared</td><td>可选</td><td>false</td><td>Boolean</td><td>确认所有 Flink 进程以相同路径共享文件系统。配合非 DuckDB Catalog 时必须为 <code>true</code>。</td></tr>
    <tr><td>storage.properties.endpoint</td><td>可选</td><td>(none)</td><td>String</td><td>S3 兼容 endpoint。<code>http://</code> 表示关闭 TLS；不带 scheme 时使用 TLS。</td></tr>
    <tr><td>storage.properties.region</td><td>可选</td><td>config 模式为 us-east-1</td><td>String</td><td>S3 region；credential chain 可在省略时自动解析。</td></tr>
    <tr><td>storage.properties.credential-provider</td><td>可选</td><td>config</td><td>String</td><td>凭证 Provider。S3 支持 <code>config</code> 和 <code>credential-chain</code>；Azure 还支持 <code>service-principal</code>。</td></tr>
    <tr><td>storage.properties.access-key</td><td>条件必填</td><td>(none)</td><td>String</td><td>静态 access key。S3 <code>config</code>、GCS 和 R2 必填。</td></tr>
    <tr><td>storage.properties.secret-key</td><td>条件必填</td><td>(none)</td><td>String</td><td>静态 secret key。S3 <code>config</code>、GCS 和 R2 必填。</td></tr>
    <tr><td>storage.properties.session-token</td><td>可选</td><td>(none)</td><td>String</td><td>可选的静态 S3 session token。</td></tr>
    <tr><td>storage.properties.credential-chain</td><td>可选</td><td>DuckDB 默认值</td><td>String</td><td>S3 或 Azure <code>credential-chain</code> 使用的分号分隔 Provider chain。</td></tr>
    <tr><td>storage.properties.profile</td><td>可选</td><td>AWS 默认值</td><td>String</td><td><code>credential-chain</code> 使用的 AWS profile。</td></tr>
    <tr><td>storage.properties.path-style-access</td><td>可选</td><td>false</td><td>Boolean</td><td>是否使用 path-style S3 URL，S3 兼容存储通常需要开启。</td></tr>
    <tr><td>storage.properties.account-id</td><td>条件必填</td><td>(none)</td><td>String</td><td>Cloudflare account ID，R2 必填。</td></tr>
    <tr><td>storage.properties.connection-string</td><td>条件必填</td><td>(none)</td><td>String</td><td>Azure connection string，Azure <code>config</code> Provider 必填。</td></tr>
    <tr><td>storage.properties.account-name</td><td>条件必填</td><td>(none)</td><td>String</td><td>Azure storage account name，<code>credential-chain</code> 和 <code>service-principal</code> 必填。</td></tr>
    <tr><td>storage.properties.tenant-id</td><td>条件必填</td><td>(none)</td><td>String</td><td>Azure tenant ID，<code>service-principal</code> 必填。</td></tr>
    <tr><td>storage.properties.client-id</td><td>条件必填</td><td>(none)</td><td>String</td><td>Azure client ID，<code>service-principal</code> 必填。</td></tr>
    <tr><td>storage.properties.client-secret</td><td>条件必填</td><td>(none)</td><td>String</td><td>Azure client secret，<code>service-principal</code> 必填。</td></tr>
    <tr><td>duckdb.extension-directory</td><td>可选</td><td>(none)</td><td>String</td><td>DuckDB 扩展目录，扩展必须与内嵌 DuckDB JDBC 的版本和平台一致。</td></tr>
    <tr><td>duckdb.extension-repository</td><td>可选</td><td>(none)</td><td>String</td><td><code>INSTALL ... FROM</code> 使用的自定义 DuckDB 扩展仓库。</td></tr>
    <tr><td>duckdb.memory-limit</td><td>可选</td><td>512MB</td><td>String</td><td>每个独立打开的 DuckDB Client 的内存上限。本地 DuckDB Catalog lease 共享同一 Client 和上限。</td></tr>
    <tr><td>duckdb.temp-directory</td><td>可选</td><td>JVM 临时目录</td><td>String</td><td>每个独立 DuckDB Client 的 spill 根目录。本地 Catalog lease 共享该目录。</td></tr>
    <tr><td>sink.commit.max-retries</td><td>可选</td><td>8</td><td>Integer</td><td>Catalog 冲突的最大重试次数，不能为负数。</td></tr>
    <tr><td>sink.commit.retry-backoff</td><td>可选</td><td>100 ms</td><td>Duration</td><td>Catalog 冲突的初始指数退避时间，不能为负数。</td></tr>
    <tr><td>sink.orphan-cleanup.interval</td><td>可选</td><td>0 ms</td><td>Duration</td><td>尽力清理孤儿文件的执行间隔，零表示关闭自动清理。</td></tr>
    <tr><td>sink.orphan-cleanup.retention</td><td>可选</td><td>7 d</td><td>Duration</td><td>未注册文件清理前的最短保留时间，必须长于作业暂停后的最大恢复窗口。</td></tr>
    <tr><td>sink.writer.max-events-per-file</td><td>可选</td><td>100000</td><td>Long</td><td>单个 Writer 为单表滚动文件前最多缓冲的 CDC 事件数。</td></tr>
    <tr><td>sink.writer.max-buffered-events</td><td>可选</td><td>1000000</td><td>Long</td><td>单个 Writer 在所有表之间最多缓冲的 CDC 事件数；达到上限时滚动最近最少使用的表缓冲区。</td></tr>
    <tr><td>sink.writer.max-open-tables</td><td>可选</td><td>128</td><td>Integer</td><td>单个 Writer 同时打开的表缓冲区上限；打开新表缓冲区前滚动最近最少使用的缓冲区。</td></tr>
    <tr><td>sink.id-prefix</td><td>可选</td><td>flink-cdc-</td><td>String</td><td>稳定 Writer 标识的前缀，该标识保存在 checkpoint state 和 commit marker 中。</td></tr>
  </tbody>
</table>
</div>

使用说明
--------

* 每张目标表必须至少定义一个主键列。

* 主键列必须为 `NOT NULL`。

* 修改主键值的 `UPDATE` 会被拒绝，请在源端将这类变更表示为 delete 加 insert。

* 每个 Writer 只为分配给自己的记录生成文件，不会改写其他 Writer 产生的文件。

* 连接器构建的是高吞吐当前状态镜像，不保留逐行变更历史。

数据类型映射
----------------

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
  <thead>
    <tr>
      <th class="text-left">Flink CDC 类型</th>
      <th class="text-left">DuckDB/DuckLake 类型</th>
      <th class="text-left" style="width:60%;">说明</th>
    </tr>
  </thead>
  <tbody>
    <tr><td>CHAR、VARCHAR、STRING</td><td>VARCHAR</td><td>DuckLake 不强制长度。</td></tr>
    <tr><td>BOOLEAN</td><td>BOOLEAN</td><td></td></tr>
    <tr><td>BINARY、VARBINARY、BYTES</td><td>BLOB</td><td>DuckLake 不强制长度。</td></tr>
    <tr><td>DECIMAL(p, s)</td><td>DECIMAL(p, s)</td><td>最大精度为 38。</td></tr>
    <tr><td>TINYINT</td><td>TINYINT</td><td></td></tr>
    <tr><td>SMALLINT</td><td>SMALLINT</td><td></td></tr>
    <tr><td>INTEGER</td><td>INTEGER</td><td></td></tr>
    <tr><td>BIGINT</td><td>BIGINT</td><td></td></tr>
    <tr><td>FLOAT</td><td>FLOAT</td><td></td></tr>
    <tr><td>DOUBLE</td><td>DOUBLE</td><td></td></tr>
    <tr><td>DATE</td><td>DATE</td><td></td></tr>
    <tr><td>TIME(p)</td><td>TIME</td><td>最大精度为 6。</td></tr>
    <tr><td>TIMESTAMP(0)</td><td>TIMESTAMP_S</td><td></td></tr>
    <tr><td>TIMESTAMP(1..3)</td><td>TIMESTAMP_MS</td><td></td></tr>
    <tr><td>TIMESTAMP(4..6)</td><td>TIMESTAMP</td><td></td></tr>
    <tr><td>TIMESTAMP(7..9)</td><td>TIMESTAMP_NS</td><td></td></tr>
    <tr><td>TIMESTAMP_LTZ(p)</td><td>TIMESTAMPTZ</td><td>最大精度为 6；转换使用 <code>pipeline</code> 部分的 <code>local-time-zone</code>。</td></tr>
  </tbody>
</table>
</div>

不支持 `TIMESTAMP WITH TIME ZONE`、`ARRAY`、`MAP`、`ROW` 和 `VARIANT`。
仅支持物理列，并拒绝列默认值表达式。

Schema Evolution
----------------

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
  <thead>
    <tr>
      <th class="text-left">Schema 变更</th>
      <th class="text-left">是否支持</th>
      <th class="text-left" style="width:60%;">限制</th>
    </tr>
  </thead>
  <tbody>
    <tr><td>Create table</td><td>支持</td><td>必须有非空主键、仅包含物理列、类型受支持且没有列默认值。</td></tr>
    <tr><td>Add column</td><td>支持</td><td>新列必须 nullable、没有默认值，并添加在 <code>LAST</code>。</td></tr>
    <tr><td>Drop column</td><td>支持</td><td>不能删除主键列。</td></tr>
    <tr><td>Alter column type</td><td>支持</td><td>仅支持 <code>TINYINT</code> → <code>SMALLINT</code>/<code>INTEGER</code>/<code>BIGINT</code>、<code>SMALLINT</code> → <code>INTEGER</code>/<code>BIGINT</code>、<code>INTEGER</code> → <code>BIGINT</code> 和 <code>FLOAT</code> → <code>DOUBLE</code>，不能修改主键列。</td></tr>
    <tr><td>Rename column</td><td>支持</td><td>不能重命名主键列；目标列名必须唯一；不支持链式或循环 rename，也不能复用曾经删除的列名。connector 会在表末尾物化重命名后的列，因此该操作会改变 <code>SELECT *</code> 的列顺序，并扫描和更新整张表。</td></tr>
    <tr><td>Rename table</td><td>不支持</td><td>请路由到新的目标表。</td></tr>
    <tr><td>Truncate/drop table</td><td>不支持</td><td>请在连接器外部执行生命周期管理。</td></tr>
  </tbody>
</table>
</div>

不支持三段式目标表标识。源表进入 Sink 前必须路由为两段式 `schema.table` 目标表。
`_flink_cdc_internal` 和 `_flink_cdc_staging` schema 分别保留用于 connector
元数据和存储暂存文件。

Checkpoint、可见性与文件生命周期
----------------

Writer 将不可变数据文件写入
`<data-path>/<schema>/<table>/flink-cdc/.../writer-epoch-<n>`，将主键暂存文件写入
`<data-path>/_flink_cdc_staging/keys/.../writer-epoch-<n>`。schema 和 table 路径分量
使用百分号编码。checkpoint commit 时，committer 读取主键文件删除旧行，在原路径向
DuckLake 注册非空数据文件，并在同一个 catalog 事务中写入 checkpoint marker。
使用相同 marker 和 plan hash 的重复提交是 no-op；同一 marker 对应不同 plan 时会失败。

Schema 事件到达 Sink 时，每个 Writer 会先刷新使用旧 Schema 写入的数据，再将
Schema 事件保存为 checkpoint committable，然后开始新的 schema batch。
MetadataApplier 只同步执行能力和安全性校验，不会修改 DuckLake Catalog。

对于每张表，全局分区后的 commit 流按 schema batch 顺序执行一个 checkpoint：先注册
使用旧 Schema 写入的文件，再执行 Schema 变更，最后注册使用新 Schema 写入的文件。
文件注册、旧行删除、Schema 变更和 checkpoint marker 都通过 DuckDB JDBC 在同一个
DuckLake 事务中完成。因此，执行 DDL 时发生的 Catalog 错误会使 checkpoint commit
失败，并进入与数据提交失败相同的重试和恢复流程。

DuckLake 内部 schema `_flink_cdc_internal` 保存 checkpoint marker 和源表主键元数据。
Committer 使用这些元数据，在恢复后继续校验涉及主键的 Schema 变更。

不存在 staging 到 final 的文件复制。文件一旦注册，表目录中的不可变文件即归
DuckLake 所有，是 DuckLake 正在使用的数据文件，connector 不得修改或删除。

connector 不会直接从存储删除主键文件或未提交/orphan 数据文件。当
`sink.orphan-cleanup.interval` 大于零时，committer 会定期调用 DuckLake 的
`ducklake_delete_orphaned_files` 维护函数；关闭自动清理时，应从外部调用同一函数。
两种方式都必须将保留时间设置为长于最大 checkpoint 恢复窗口。该函数会删除满足条件且
未被 catalog 跟踪的文件，并保留 catalog 仍引用的文件。不得对表数据目录设置独立的存储
生命周期删除规则；staging 前缀仅可使用保留时间长于最大恢复窗口的生命周期策略。

checkpoint 事务提交后，数据变更、受支持的 DDL 和 checkpoint marker 原子可见。
rename 之前写好的文件会先注册。随后 Committer 新增目标列、复制源列数据、恢复目标列的
nullable 约束并删除源列，最后注册使用新列名写入的文件。目标列会追加到表末尾，因此
下游不能假设 `SELECT *` 仍保持原列顺序。该物化 rename 不依赖跨多个 DuckLake schema
version 的 name mapping，但 I/O 与表中已有数据量成正比，并可能在 DuckLake change
feed 中表现为全表更新。大表 rename 应安排在合适的维护窗口执行。

{{< top >}}
