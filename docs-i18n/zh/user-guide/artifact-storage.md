---
translation_status: translated
---

# 制品存储 { #artifact-storage }

GPUStack 将大型私有 JSON 制品存储在兼容 S3 的存储桶中。Benchmark 监控归档使用这一存储层；浏览器客户端不会获得存储凭据。PostgreSQL/MySQL 元数据与对象存储必须一并备份。

## 模式 { #modes }

- `builtin`：打包的 Linux 容器在同一节点上启动 **SeaweedFS 4.54**。这是打包服务器的默认模式。请持久化整个 GPUStack 数据卷（默认为 `/var/lib/gpustack`），包括 `artifacts/builtin`。
- `external`：使用共享 S3 服务和已有存储桶。GPUStack 不会创建存储桶，也不会修改其策略、加密、生命周期或公共访问设置。
- `disabled`：停止写入新制品。显式保留的读取配置仍可供读取方使用。这是源码/wheel 安装和工作节点的默认模式。

多服务器部署必须选择 `external`，并共享存储桶和命名空间。打包的单节点服务不提供存储复制或高可用保证。凭据仅在首次使用时随机生成，作为私有文件保存到持久卷中，并在重启后复用。S3 和内部端点均绑定到回环地址；无需额外发布容器端口。

## 配置 { #configuration }

通过 YAML 文件、`GPUSTACK_` 环境变量或 CLI 参数配置服务器（例如 `--artifact-storage-mode external`）。YAML 键使用下划线，命令行参数使用连字符，环境变量名使用大写字母和下划线。

```yaml
artifact_storage_mode: external
artifact_storage_profile_id: monitoring-eu-v1
artifact_storage_endpoint: https://s3.example.com # omit for AWS endpoint discovery
artifact_storage_bucket: gpustack-private
artifact_storage_prefix: installations/production
artifact_storage_region: eu-west-1
artifact_storage_addressing_style: path # auto, path, virtual
artifact_storage_tls_verify: true
# artifact_storage_ca_file: /etc/ssl/private/s3-ca.pem
```

未提供显式凭据时，使用 SDK 默认的 AWS 凭据链。这支持环境凭据、共享 AWS 配置，以及 SDK 支持的实例、容器或 Web 身份凭据。实际的云身份链需要在目标部署环境中验证。显式凭据使用 `artifact_storage_access_key_id`、`artifact_storage_secret_access_key` 和可选的 `artifact_storage_session_token`。请通过受保护的配置或机密环境注入提供这些值。GPUStack Config 的转储和表示中不会包含它们。不要将凭据放入端点 URL。

默认启用 TLS 验证，自定义端点也不例外。使用 `artifact_storage_ca_file` 设置自定义 CA。禁用证书验证需要显式设置 `artifact_storage_tls_verify: false` 或使用 `--no-artifact-storage-tls-verify` 参数。

| 设置 | 默认值 | 含义 |
| --- | --- | --- |
| `artifact_storage_connect_timeout` | 5 秒 | SDK 连接超时 |
| `artifact_storage_read_timeout` | 30 秒 | SDK 读取超时 |
| `artifact_storage_max_attempts` | 3 | SDK 尝试总次数，包括首次尝试 |
| `artifact_storage_max_connections` | 8 | SDK 连接池大小 |
| `artifact_storage_max_concurrency` | 4 | 每个配置和服务器事件循环中的并发调用数 |
| `artifact_storage_max_object_bytes` | 16777216 | 单个文档解码后的最大字节数 |
| `artifact_storage_max_total_bytes` | 0（无限制） | 整个安装实例的初始压缩对象容量；仅初始化一次共享数据库策略 |
| `artifact_storage_builtin_port` | 18333 | builtin 模式下的回环 S3 端口 |
| `benchmark_archive_retention_days` | 未设置 | 遵循 Benchmark 保留策略；可选的归档过期天数 |
| `benchmark_archive_sample_step_seconds` | 15 | 归档采集器的采样网格间隔 |
| `benchmark_archive_flush_interval_seconds` | 60 | 归档刷新间隔 |
| `benchmark_archive_settle_seconds` | 30 | 等待源指标稳定的时间 |
| `benchmark_archive_max_concurrency` | 2 | 归档任务并发预算 |
| `benchmark_archive_max_run_bytes` | 268435456 | 每次执行的最大归档字节数 |

保留和采集器设置由 Benchmark 归档域使用；S3 适配器不会静默截断过大的对象。请让存储桶自身的保留设置与应用保留策略一致，避免存储桶生命周期删除仍有索引的对象。

## 监控对象名称 { #monitoring-object-names }

新监控对象的名称在不可变 UUID 前描述其范围：

```text
resources-<uuid>.json.gz
runtime-aggregate-<uuid>.json.gz
runtime-prefill-instances-<uuid>.json.gz
phase-1024-1-runtime-decode-instances-<uuid>.json.gz
```

目录已经标识安装实例、所有者、Benchmark、执行和监控格式，因此文件名不再重复这些字段。前缀标识运行时/资源数据、特定角色和聚合/实例视图。已记录阶段会添加阶段 ID；完整运行文件省略 `run`。默认的 `all` 角色和资源范围默认的聚合视图也会省略。一个文件包含多个指标。各组成部分会规范化为长度受限的 ASCII 标签；范围仍以数据库为准。UUID 可防止不同采集结果相互覆盖。

已有的纯 UUID 名称保持不变，仍可读取。读取、迁移和清理均使用索引中完整的对象键，绝不会根据 UUID 重建路径。文件名不标识活动修订版，也不保证样本完整。这些属性仍保存在共享归档索引中。

## 可读修订版清单 { #readable-revision-manifests }

归档封存为 `ready`、`partial` 或 `no_data` 后，独立的后台循环会为每个已发布修订版创建目录：

```text
installations/<installation>/owners/<owner>/benchmarks/<id>/runs/<run>/
  monitoring/v1/manifests/revision-000001.json
```

这些是带缩进的未压缩 UTF-8 JSON 文件，因此可以在 S3 或 Filer 浏览器中显示。监控曲线仍保存为独立的 gzip 对象。旧的已封存归档会根据其已发布数据库索引回填，无需查询 Prometheus 或更改监控数据。正在运行的采集会等待封存；可选的目录写入不会阻塞正在进行的采集循环。

每个文档标识其不可变修订版，并列出范围、阶段、角色、视图、指标名称/单位、样本覆盖情况、对象键、大小和 SHA-256 值。受支持的指标具有详细元数据；`unavailable_metrics` 将不支持和不适用的键及原因紧凑分组。受支持但无样本的指标仍单独区分。时间保留 Unix 秒数，并包含可读的 UTC 时间戳。`revision_state` 描述保存的修订版，而不是当前任务状态。原始提示词、凭据、启动命令、环境变量或私有源地址都不会复制到目录中。

首次准备目录时会冻结 `stored_bytes` 值。后续配置迁移可能使用另一种 gzip 编码，但仍保留相同数据。对于曲线对象，`checksum_scope: uncompressed_json_bytes` 表示 SHA-256 针对解码后的 JSON 字节计算，而不是下载的 `.json.gz` 文件。对于清单本身，校验和覆盖其实际 UTF-8 JSON 字节。

活动修订版及其清单是否已提交由数据库决定；不存在可变的 `latest.json` 指针。上传可能已完成，而数据库确认随后失败，因此仅有文件并不能证明其清单引用已提交。正常经过身份验证的 `GET /v2/benchmarks/<id>/monitoring-archive` 响应包含一个 `manifest` 对象：

```json
{
  "state": "ready",
  "archive_revision": 1,
  "filename": "revision-000001.json",
  "size_bytes": 12345,
  "sha256": "<sha256>",
  "checksum_scope": "utf8_json_bytes"
}
```

同一字段也会报告 `pending`、附带安全 `last_error` 的 `failed`，或 `unavailable`。它不会暴露私有存储桶/配置设置或完整对象键。目录失败（包括容量拒绝）不会改变采集状态，也不会使已保存的图表失效；后台重试可在稍后完成写入。

清单使用共享字节账本，并参与配置迁移、完整性审计、Benchmark 删除和归档过期清理。它们不占用监控的单次运行字节预算。上传结果未知时会保留索引中的写入意图和预留量，并由后续正常重试进行协调。固定清单键不参与常规孤立对象/容量清理，以防过期的清理器删除重试发布的内容；显式删除归档或归档过期仍会将它们全部移除。被拒绝且从未尝试的上传不会留下孤立对象。

## 全局容量 { #global-capacity }

容量限制适用于此 GPUStack 安装实例的所有服务器及其配置的存储配置。它统计托管对象的压缩字节数，包括保留的归档修订版、未发布对象，以及配置迁移期间的两份副本。它也统计为 S3 结果尚未确认的写入预留的空间。不计入存储桶中的其他文件、对象存储复制开销、保留的 S3 对象版本，以及 SeaweedFS 尚未回收的文件系统空间。

默认**无限制**。首次初始化时，在 YAML 中设置 `artifact_storage_max_total_bytes`、在环境中设置 `GPUSTACK_ARTIFACT_STORAGE_MAX_TOTAL_BYTES`，或在服务器启动时使用 `--artifact-storage-max-total-bytes` 来提供策略初始值。值为整数字节数；`0` 表示无限制。支持的最大配置限制为 `9007199254740991` 字节。所有服务器应使用相同的初始配置。初始化后以共享数据库策略为准：使用不同初始值重启服务器不会覆盖管理员保存的策略。

平台管理员可以通过 `GET` / `PUT /v2/artifact-storage/capacity` 查看或更改活动策略，而无需重启服务器；PUT 接受 `{"max_bytes": 0}` 表示无限制，也可接受正的字节限制。响应包含 `used_bytes`、`reserved_bytes`、`total_bytes`、`remaining_bytes`、`usage_percent` 和 `state`。无限制模式下，剩余字节数和使用百分比返回 null。有限制模式在达到 80% 时发出警告，达到或超过上限时变为 `full`。

CLI 提供相同的共享策略：

```sh
gpustack artifact-storage --config-file /etc/gpustack/config.yaml capacity
gpustack artifact-storage --config-file /etc/gpustack/config.yaml \
  capacity --max-bytes 107374182400
gpustack artifact-storage --config-file /etc/gpustack/config.yaml \
  capacity --max-bytes 0
```

上传前会在共享数据库中串行化预留操作。空间不足的写入报告 `storage_capacity_exceeded`；它不能静默驱逐历史数据或截断归档。已有归档仍可读取，Benchmark 请求执行和客户端统计继续独立运行。提高上限、选择无限制或删除托管归档，可使待处理采集恢复。遗漏的源时间区间只有在 Prometheus 仍保留该区间时才能恢复。允许将上限降至当前使用量以下，此时会阻止进一步分配，但不会删除已有数据。

只有 S3 确认删除后才释放空间。失败或中断的上传会保守地保留预留量，直到协调或清理确认结果。已有对象在首次使用时导入账本；压缩大小未知的旧预期对象或迁移副本，会按未压缩大小保守预留。这可能导致报告的已分配空间暂时高于实际存储字节数。这不是整个存储桶的使用量扫描，也不是 S3 生命周期策略控制器。存储桶版本控制、对象锁定和物理磁盘回收仍由存储管理员负责。

## 采集状态与源覆盖情况 { #capture-status-and-source-coverage }

`capture_complete` 描述固定读取的归档修订版。对于 `ready` 和 `no_data` 修订版，其值为 true；对于有采集错误、身份失效或测量范围不可用的 `partial` 修订版，其值为 false。`archive_state` 报告当前任务状态；客户端持续读取旧修订版时，该状态仍可能变化。

`data_coverage` 单独描述已保存的源观测数据。如果首次采样时刻早于首次抓取，或滚动延迟窗口过短，短阶段可以为 `ready`，同时具有 `capture_complete: true` 和 `data_coverage: partial`。这些情况会保留各自的指标状态、null、零值及覆盖统计，无需重试已经成功的采集。查询失败仍属于可重试的采集错误。重试成功会发布新修订版，并为固定修订版的读取方保留先前修订版。

## 存储配置与变更 { #storage-profiles-and-changes }

配置 ID 永久标识端点、存储桶、前缀和区域。适配器在 `artifacts/profiles` 下存储指纹绑定，并拒绝将该 ID 复用于不同目标。归档索引也保留配置身份。同一目标的凭据轮换无需新 ID。

更改目标时，请选择新的写入配置 ID，并在 `artifact_storage_read_profiles` 下保留旧连接。配置条目使用相同设置，但不带 `artifact_storage_` 前缀。通过 YAML 或 JSON 环境变量 `GPUSTACK_ARTIFACT_STORAGE_READ_PROFILES` 配置此映射。

```yaml
artifact_storage_profile_id: monitoring-eu-v2
artifact_storage_bucket: new-private-bucket
artifact_storage_read_profiles:
  monitoring-eu-v1:
    endpoint: https://old-s3.example.com
    bucket: gpustack-private
    prefix: installations/production
    region: eu-west-1
    addressing_style: path
    # Credentials may be omitted to use the SDK credential chain.
```

在所有依赖归档完成迁移或删除之前，保持旧配置及其对象可访问。切换到 external 或 disabled 模式会停止打包的 SeaweedFS 服务；请先迁移本地对象，或保留可独立访问的服务供旧数据读取。仅将文件复制到新存储桶不会重写归档索引。绝不要仅为使变更后的目标通过检查而删除配置绑定。

## 所需 S3 操作 { #required-s3-operations }

适配器使用 SigV4 `HeadObject`、`PutObject`、`GetObject` 和 `DeleteObject`。仅授予运行时访问其配置命名空间的权限。支持时使用带有 `If-None-Match: *` 的条件 `PutObject`。`artifact_storage_conditional_writes: false` 会显式选择可移植 S3 模式；收到明确表示不支持条件的响应时，适配器也会回退。权限错误和并发冲突绝不会触发回退。可移植写入会检查已有内容，但无法让 HEAD 与 PUT 成为原子操作：调用方必须像 Benchmark 归档器一样，使用唯一对象键和受数据库隔离令牌保护的发布流程。相同内容的重试会验证已存储的校验和。ETag 是不透明值，绝不将其视为 MD5 校验和。

只读连接检查使用 `HeadBucket`，在 AWS 上需要 `s3:ListBucket`。因此，即使允许对象操作，检查被拒绝也可能表示缺少健康检查权限。该检查不会创建存储桶或更改数据。仅 builtin 初始化会为其托管存储桶使用 `CreateBucket`。

每个文档独立进行 gzip 压缩，并附带针对规范化、未压缩 UTF-8 JSON 的 SHA-256。读取会验证存储的元数据和索引中预期的校验和，限制压缩及解码后的字节数，并关闭响应流。所有 SDK I/O 和压缩均在事件循环之外运行。错误仅暴露稳定且安全的原因，不包含 SDK 异常正文或凭据。

存储连接参数、环境变量和 YAML 设置见 [CLI 配置参考](../cli-reference/start.md#artifact-storage-options)。External 模式连接到已有的兼容 S3 服务；GPUStack 不部署或管理该服务的副本、元数据数据库或可用性。

打包服务使用 HTTP 端口 18333（S3）、19333（master）、18080（volume）、18888（filer），以及内部 gRPC 端口 29333、28080、28888。它们全部绑定到回环地址。仅 S3 端口可配置。master 数据、对象数据、Filer LevelDB 和凭据均位于 `artifacts/builtin` 下；仅备份部分卷是不够的。

## 验证范围 { #verification-scope }

单元测试无需网络，覆盖字节边界、校验和、不可变重试和竞争写入、安全错误、配置、凭据持久化、禁用模式、读取配置及 SDK 任务卸载。SeaweedFS 4.54 的本地冒烟测试验证 S3 上传、下载、重启后持久化、校验和验证及删除。两种架构上的 Linux 镜像构建，以及 AWS/其他厂商身份、TLS、权限、性能和故障场景，需要在部署层面验证。

固定版本二进制文件的参数和校验和遵循[官方 4.54 发布](https://github.com/seaweedfs/seaweedfs/releases/tag/4.54)、[server 命令](https://github.com/seaweedfs/seaweedfs/blob/4.54/weed/command/server.go)和 [S3 配置文档](https://github.com/seaweedfs/seaweedfs/wiki/Amazon-S3-API)。

## 配置迁移与完整性审计 { #profile-migration-and-integrity-audit }

管理员 CLI 直接使用配置的数据库连接。它打开已有模式，不会创建表、运行迁移、初始化身份验证提供方或启动存储服务。运行前请配置旧配置和目标配置，并对数据库与对象进行协调一致的备份。

预览选定的执行，不上传数据，也不更改数据库：

```sh
gpustack artifact-storage --config-file /etc/gpustack/config.yaml \
  migrate-profile --source-id monitoring-eu-v1 --target-id monitoring-eu-v2 \
  --benchmark-id 7 --run-id RUN_UUID --dry-run --manifest /secure/migration-plan.json
```

使用 `--apply` 应用同一选择。不指定两个操作参数时也会执行试运行。`--archive-id`、`--benchmark-id` 和 `--run-id` 可缩小选择范围；`--limit` 限制单次调用的数量（默认 100，最多 1000 个归档）。命令仅处理处于 `ready`、`partial` 或 `no_data` 状态的归档，并拒绝认领具有活动租约的归档。它不会从 Prometheus 回填数据，也不会迁移未完成/失败的采集。

对每个归档，迁移会获取现有数据库租约，递增其隔离令牌，并在上传前持久化预期的源/目标对象引用。它复用目标配置中的稳定对象键，验证源和目标 SHA-256，按对象保存进度检查点，并在所有对象验证通过后，通过单个受隔离令牌保护的事务更新归档及其已发布分块的配置 ID。范围 ID、归档修订版、时间戳和载荷校验和保持不变，因此已发布修订版会重放相同数据。不同归档独立提交；失败的批次中也可能已有完成的归档。

中断后重新运行同一命令。已完成归档不再匹配源配置，未完成副本从持久化进度继续。租约丢失会阻止发布，其他进程可在租约到期后恢复（最长三分钟）。缺失的目标副本会根据已验证的源重新创建；损坏或冲突的副本会停止迁移，等待管理员调查。源副本在整个过程中保持完整，成功迁移后也不例外。

保留的源副本和未完成的目标副本仍在归档迁移历史中保留索引。正常的 Benchmark 删除或配置的归档过期，会通过同一个可重试 GC 任务清理它们。在清理完成前，请保留其存储配置。系统不会隐式立即清除源副本，也不提供整个存储桶的清理命令。请为临时的重复存储预留容量。

审计当前已索引对象，不更改存储或数据库行：

```sh
gpustack artifact-storage --config-file /etc/gpustack/config.yaml \
  audit --profile-id monitoring-eu-v2 --manifest /secure/integrity-report.json
```

`check` 是 `audit` 的别名。相同的归档/运行/Benchmark 筛选条件和数量限制同样适用。报告标识缺失对象、校验和不匹配、不可用配置及安全的存储失败原因，不包含载荷或凭据。退出状态：成功为 0，审计发现问题为 2，操作失败为 1。提供 `--manifest` 时，报告以私有权限写入。审计检查已发布且有索引的对象，不会扫描整个存储桶，也不会猜测哪些未索引对象可以删除。
