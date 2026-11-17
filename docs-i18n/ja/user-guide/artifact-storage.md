---
translation_status: translated
---

# アーティファクトストレージ { #artifact-storage }

GPUStack は、大きな非公開 JSON アーティファクトを S3 互換バケットに保存します。Benchmark の監視アーカイブはこの層を使用し、ブラウザークライアントにストレージの認証情報が渡されることはありません。PostgreSQL/MySQL のメタデータとオブジェクトストレージは、一緒にバックアップする必要があります。

## モード { #modes }

- `builtin`：パッケージ化された Linux コンテナーが同じノードで **SeaweedFS 4.54** を起動します。パッケージ版サーバーの既定のモードです。`artifacts/builtin` を含む GPUStack データボリューム全体（既定では `/var/lib/gpustack`）を永続化してください。
- `external`：共有 S3 サービスと既存のバケットを使用します。GPUStack はバケットを作成せず、ポリシー、暗号化、ライフサイクル、公開アクセスも変更しません。
- `disabled`：新しいアーティファクトの書き込みを停止します。明示的に保持された読み取りプロファイルは、引き続き読み取りに使用できます。ソース/wheel からのインストールとワーカーでは、これが既定のモードです。

複数サーバーのデプロイでは `external` を選び、バケットと名前空間を共有する必要があります。パッケージ版の単一ノードサービスは、ストレージのレプリケーションや高可用性を保証しません。認証情報は初回に一度だけランダムに生成され、永続ボリューム上の非公開ファイルに保存されて、再起動後も再利用されます。S3 および内部エンドポイントはループバックにバインドするため、追加のコンテナーポートを公開する必要はありません。

## 設定 { #configuration }

YAML ファイル、`GPUSTACK_` 環境変数、または CLI フラグ（例：`--artifact-storage-mode external`）でサーバーを設定します。YAML キーはアンダースコア、フラグはハイフン、環境変数名は大文字とアンダースコアを使用します。

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

認証情報を明示しない場合は、SDK の既定の AWS 認証情報チェーンを使用します。環境変数の認証情報、共有 AWS 設定、および SDK が対応するインスタンス、コンテナー、Web ID の認証情報を利用できます。実際のクラウド ID チェーンは、対象のデプロイ環境で検証する必要があります。認証情報を明示する場合は、`artifact_storage_access_key_id`、`artifact_storage_secret_access_key`、および任意の `artifact_storage_session_token` を使用します。保護された設定またはシークレットの環境変数注入を通じて指定してください。これらは GPUStack Config のダンプや文字列表現から除外されます。エンドポイント URL に認証情報を含めないでください。

カスタムエンドポイントを含め、TLS 検証は既定で有効です。カスタム CA は `artifact_storage_ca_file` で指定します。証明書検証を無効にするには、`artifact_storage_tls_verify: false` を明示的に設定するか、`--no-artifact-storage-tls-verify` フラグを指定する必要があります。

| 設定 | 既定値 | 意味 |
| --- | --- | --- |
| `artifact_storage_connect_timeout` | 5 秒 | SDK の接続タイムアウト |
| `artifact_storage_read_timeout` | 30 秒 | SDK の読み取りタイムアウト |
| `artifact_storage_max_attempts` | 3 | 初回を含む SDK の総試行回数 |
| `artifact_storage_max_connections` | 8 | SDK 接続プールのサイズ |
| `artifact_storage_max_concurrency` | 4 | プロファイルおよびサーバーのイベントループごとの実行中の呼び出し数 |
| `artifact_storage_max_object_bytes` | 16777216 | 1 文書をデコードした後の最大バイト数 |
| `artifact_storage_max_total_bytes` | 0（無制限） | インストール全体の圧縮オブジェクト容量の初期値。共有データベースポリシーを一度だけ初期化します |
| `artifact_storage_builtin_port` | 18333 | builtin モードのループバック S3 ポート |
| `benchmark_archive_retention_days` | 未設定 | Benchmark の保持期間に従います。日数によるアーカイブの有効期限を任意で指定できます |
| `benchmark_archive_sample_step_seconds` | 15 | アーカイブコレクターのサンプリング間隔 |
| `benchmark_archive_flush_interval_seconds` | 60 | アーカイブをフラッシュする間隔 |
| `benchmark_archive_settle_seconds` | 30 | ソースメトリクスの確定を待つ時間 |
| `benchmark_archive_max_concurrency` | 2 | アーカイブジョブの同時実行数の上限 |
| `benchmark_archive_max_run_bytes` | 268435456 | 実行ごとのアーカイブの最大バイト数 |

保持期間とコレクターの設定は Benchmark アーカイブドメインで使用されます。S3 アダプターが大きすぎるオブジェクトを黙って切り詰めることはありません。索引に残っているオブジェクトがバケットのライフサイクルによって削除されないよう、バケット独自の保持設定をアプリケーションの保持設定に合わせてください。

## 監視オブジェクトの名前 { #monitoring-object-names }

新しい監視オブジェクトの名前は、不変の UUID の前にそのスコープを表します。

```text
resources-<uuid>.json.gz
runtime-aggregate-<uuid>.json.gz
runtime-prefill-instances-<uuid>.json.gz
phase-1024-1-runtime-decode-instances-<uuid>.json.gz
```

ディレクトリーですでにインストール、所有者、Benchmark、実行、監視形式を識別しているため、ファイル名ではそれらを繰り返しません。接頭辞はランタイム/リソースデータ、特定のロール、集約/インスタンスビューを識別します。記録されたフェーズにはフェーズ ID が付き、実行全体のファイルでは `run` を省略します。既定の `all` ロールと、リソーススコープの既定の集約ビューも省略します。1 ファイルに複数のメトリクスが含まれます。各要素は長さを制限した ASCII ラベルに正規化されますが、スコープは引き続きデータベースを正とします。UUID により、異なる収集結果が互いに上書きされるのを防ぎます。

既存の UUID だけの名前は変更されず、引き続き読み取れます。読み取り、移行、クリーンアップは索引にある完全なオブジェクトキーを使用し、UUID からパスを再構築することはありません。ファイル名は有効なリビジョンを特定するものでも、サンプルの完全性を保証するものでもありません。これらの属性は共有アーカイブ索引に保持されます。

## 読み取り可能なリビジョンマニフェスト { #readable-revision-manifests }

アーカイブが `ready`、`partial`、または `no_data` として封印されると、独立したバックグラウンドループが公開済みリビジョンごとに目録を作成します。

```text
installations/<installation>/owners/<owner>/benchmarks/<id>/runs/<run>/
  monitoring/v1/manifests/revision-000001.json
```

これらはインデント付きの非圧縮 UTF-8 JSON ファイルなので、S3 または Filer のブラウザーで表示できます。監視曲線は引き続き独立した gzip オブジェクトです。古い封印済みアーカイブは、Prometheus に問い合わせたり監視データを変更したりせず、公開済みのデータベース索引から補完されます。実行中の収集は封印されるまで待機します。任意の目録書き込みが進行中の収集ループをブロックすることはありません。

各文書は不変のリビジョンを識別し、スコープ、フェーズ、ロール、ビュー、メトリクス名/単位、サンプルのカバレッジ、オブジェクトキー、サイズ、SHA-256 値を列挙します。対応しているメトリクスには詳細なメタデータがあり、`unavailable_metrics` は非対応または適用対象外のキーとその理由を簡潔にグループ化します。対応していてサンプルがないメトリクスは、これらとは区別されます。時刻は Unix 秒を保持し、可読な UTC タイムスタンプも含みます。`revision_state` は現在のジョブ状態ではなく、その保存済みリビジョンを表します。生のプロンプト、認証情報、起動コマンド、環境変数、非公開のソースアドレスは目録にコピーされません。

`stored_bytes` の値は、目録を最初に準備した時点で固定されます。後のプロファイル移行では、データを保持したまま別の gzip エンコーディングを使用することがあります。曲線オブジェクトの `checksum_scope: uncompressed_json_bytes` は、SHA-256 がダウンロードした `.json.gz` ではなく、デコード後の JSON バイト列に対するものであることを意味します。マニフェスト自体のチェックサムは、その実際の UTF-8 JSON バイト列を対象とします。

有効なリビジョンとマニフェストのコミット状況はデータベースが決定します。変更可能な `latest.json` ポインターはありません。アップロード後にデータベースの確認が失敗する可能性があるため、ファイルの存在だけではマニフェスト参照がコミットされた証明にはなりません。通常の認証済み `GET /v2/benchmarks/<id>/monitoring-archive` レスポンスには `manifest` オブジェクトが含まれます。

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

同じフィールドは `pending`、安全な `last_error` を伴う `failed`、または `unavailable` も報告します。非公開のバケット/プロファイル設定や完全なオブジェクトキーは公開しません。容量超過による拒否を含む目録作成の失敗は、収集状態を変更せず、保存済みチャートも無効にしません。バックグラウンドの再試行によって、後で完了できます。

マニフェストは共有バイト台帳を使用し、プロファイル移行、整合性監査、Benchmark の削除、アーカイブの有効期限切れに伴う処理に参加します。監視の実行ごとのバイト予算は消費しません。アップロード結果が不明な場合は、索引に記録した意図と予約領域を保持し、正常な再試行で整合させます。固定マニフェストキーは通常の孤立オブジェクト/容量クリーンアップから除外され、古い状態のクリーナーが再試行された公開物を削除することを防ぎます。明示的なアーカイブ削除や有効期限切れでは、引き続きすべて削除されます。拒否され、一度も試行されなかったアップロードは、孤立オブジェクトを残しません。

## 全体の容量 { #global-capacity }

容量は、この GPUStack インストールのすべてのサーバーと設定済みストレージプロファイルに適用されます。管理対象オブジェクトの圧縮バイト数を数え、保持されているアーカイブリビジョン、未公開オブジェクト、プロファイル移行中の両方のコピーを含みます。S3 での結果が未確認の書き込みに予約された領域も数えます。バケット内のその他のファイル、オブジェクトストレージのレプリケーションによる追加領域、保持された S3 オブジェクトバージョン、SeaweedFS がまだ回収していないファイルシステム領域は含みません。

既定では**無制限**です。初回の初期化時にポリシーの初期値を設定するには、YAML の `artifact_storage_max_total_bytes`、環境変数の `GPUSTACK_ARTIFACT_STORAGE_MAX_TOTAL_BYTES`、またはサーバー起動時の `--artifact-storage-max-total-bytes` を指定します。値は整数のバイト数で、`0` は無制限を意味します。設定可能な上限の最大値は `9007199254740991` バイトです。すべてのサーバーで同じ初期設定を使用してください。初期化後は共有データベースポリシーを正とするため、異なる初期値でサーバーを再起動しても、管理者が保存したポリシーは上書きされません。

プラットフォーム管理者は、サーバーを再起動せずに `GET` / `PUT /v2/artifact-storage/capacity` で有効なポリシーを確認または変更できます。PUT は無制限を表す `{"max_bytes": 0}`、または正のバイト上限を受け付けます。レスポンスには `used_bytes`、`reserved_bytes`、`total_bytes`、`remaining_bytes`、`usage_percent`、`state` が含まれます。無制限モードでは残りバイト数と使用率が null になります。上限付きモードは 80% で警告し、上限以上になると `full` になります。

CLI でも同じ共有ポリシーを操作できます。

```sh
gpustack artifact-storage --config-file /etc/gpustack/config.yaml capacity
gpustack artifact-storage --config-file /etc/gpustack/config.yaml \
  capacity --max-bytes 107374182400
gpustack artifact-storage --config-file /etc/gpustack/config.yaml \
  capacity --max-bytes 0
```

予約はアップロード前に共有データベースで直列化されます。空き容量に収まらない書き込みは `storage_capacity_exceeded` を報告し、履歴を黙って追い出したりアーカイブを切り詰めたりはしません。既存のアーカイブは引き続き読み取り可能で、Benchmark のリクエスト実行とクライアント統計は独立して継続します。上限を増やす、無制限を選ぶ、または管理対象アーカイブを削除することで、保留中の収集を再開できる場合があります。欠落したソース期間を復元できるのは、Prometheus がまだその期間のデータを保持している間だけです。上限を現在の使用量より低くすることは可能で、新たな割り当てはブロックされますが、既存データは削除されません。

領域を解放するのは、S3 が削除を確認した後だけです。失敗または中断したアップロードは、整合処理やクリーンアップで結果を確認するまで、安全側に予約領域を保持します。既存オブジェクトは初回使用時に台帳へ取り込まれます。圧縮サイズが不明な従来の書き込み予定オブジェクトや移行コピーには、非圧縮サイズに基づく保守的な予約量を使用します。このため、割り当て済み領域が実際の保存バイト数より一時的に多く報告されることがあります。これはバケット全体の使用量スキャンでも、S3 ライフサイクルポリシーのコントローラーでもありません。バケットのバージョニング、オブジェクトロック、物理ディスク領域の回収は、引き続きストレージ管理者の責任です。

## 収集状態とソースのカバレッジ { #capture-status-and-source-coverage }

`capture_complete` は、固定されたアーカイブリビジョンを表します。`ready` と `no_data` のリビジョンでは true、収集エラー、無効になった識別情報、または利用できない測定スコープがある `partial` リビジョンでは false です。`archive_state` は現在のジョブ状態を報告するため、クライアントが古いリビジョンを読み続けている間にも変化することがあります。

`data_coverage` は、保存されたソース観測値を別に表します。最初のサンプリング時刻が最初のスクレイプより前である場合や、移動レイテンシーウィンドウが短すぎる場合、短いフェーズは `ready` であっても、`capture_complete: true` と `data_coverage: partial` を持つことがあります。これらの状態ではメトリクスのステータス、null、ゼロ値、カバレッジ統計が維持され、成功した収集を再試行する必要はありません。クエリーの失敗は、引き続き再試行可能な収集エラーです。再試行が成功すると新しいリビジョンを公開し、固定リビジョンを読むクライアント向けに以前のリビジョンを保持します。

## ストレージプロファイルと変更 { #storage-profiles-and-changes }

プロファイル ID は、エンドポイント、バケット、接頭辞、リージョンを永続的に識別します。アダプターは `artifacts/profiles` の下にフィンガープリントの関連付けを保存し、異なる宛先への同じ ID の再利用を拒否します。アーカイブ索引にもプロファイルの識別情報が保持されます。同じ宛先の認証情報をローテーションするだけなら、新しい ID は不要です。

宛先を変更する場合は、新しい書き込みプロファイル ID を選び、`artifact_storage_read_profiles` の下に古い接続を保持してください。プロファイルの各エントリーは `artifact_storage_` 接頭辞を付けずに同じ設定を使用します。この対応関係は YAML または JSON 環境変数 `GPUSTACK_ARTIFACT_STORAGE_READ_PROFILES` で設定します。

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

依存するすべてのアーカイブが移行または削除されるまで、古いプロファイルとそのオブジェクトにアクセスできる状態を維持してください。external または disabled モードへ切り替えると、パッケージ版の SeaweedFS サービスは停止します。先にローカルオブジェクトを移行するか、古いデータの読み取り用に別途アクセス可能なサービスを保持してください。新しいバケットへファイルをコピーするだけでは、アーカイブ索引は書き換わりません。変更した宛先をチェックに通すためだけにプロファイルの関連付けを削除してはいけません。

## 必要な S3 操作 { #required-s3-operations }

アダプターは SigV4 の `HeadObject`、`PutObject`、`GetObject`、`DeleteObject` を使用します。ランタイムには、設定済みの名前空間に対するアクセス権だけを与えてください。対応している場合は、`If-None-Match: *` を付けた条件付き `PutObject` を使用します。`artifact_storage_conditional_writes: false` は、移植性のある S3 モードを明示的に選びます。条件が非対応であることを明示したレスポンスを受け取った場合も、アダプターはフォールバックします。権限エラーや並行処理の競合ではフォールバックしません。移植性のある書き込みは既存内容を確認しますが、HEAD と PUT をアトミックにはできません。そのため呼び出し側は、Benchmark アーカイバーと同じように、一意のオブジェクトキーとデータベースのフェンシングで保護された公開処理を使用する必要があります。同一内容の再試行では保存済みチェックサムを検証します。ETag は不透明な値であり、MD5 チェックサムとして扱われることはありません。

読み取り専用の接続チェックは `HeadBucket` を使用し、AWS では `s3:ListBucket` が必要です。そのため、オブジェクト操作が許可されていても、チェックの拒否はヘルスチェック権限の不足を示している場合があります。このチェックはバケットを作成せず、データも変更しません。builtin の初期化だけが、管理対象バケットに対して `CreateBucket` を使用します。

各文書は独立して gzip 圧縮され、正規化された非圧縮 UTF-8 JSON の SHA-256 を持ちます。読み取りは保存済みメタデータと索引の期待チェックサムを検証し、圧縮後とデコード後の両方のバイト数を制限して、レスポンスストリームを閉じます。すべての SDK I/O と圧縮処理はイベントループ外で実行します。エラーは安定した安全な理由のみを公開し、SDK 例外の本文や認証情報は公開しません。

ストレージ接続フラグ、環境変数、YAML 設定については、[CLI 設定リファレンス](../cli-reference/start.md#artifact-storage-options)を参照してください。External モードは既存の S3 互換サービスに接続します。GPUStack はそのサービスのレプリカ、メタデータデータベース、可用性をデプロイまたは管理しません。

パッケージ版サービスは HTTP ポート 18333（S3）、19333（master）、18080（volume）、18888（filer）、および内部 gRPC ポート 29333、28080、28888 を使用します。すべてループバックにバインドします。設定できるのは S3 ポートだけです。master データ、オブジェクトデータ、Filer LevelDB、認証情報はすべて `artifacts/builtin` の下にあるため、ボリュームの一部だけのバックアップでは不十分です。

## 検証範囲 { #verification-scope }

単体テストはネットワークを使用せずに実行し、バイト数の境界、チェックサム、不変性を守る再試行と競合する書き込み、安全なエラー、設定、認証情報の永続化、無効モード、読み取りプロファイル、SDK 処理のオフロードを検証します。SeaweedFS 4.54 のローカルスモークテストでは、S3 のアップロード、ダウンロード、再起動後の永続性、チェックサム検証、削除を確認します。両アーキテクチャーの Linux イメージビルド、および AWS/他ベンダーの ID、TLS、権限、性能、障害シナリオは、デプロイ環境での検証が必要です。

固定バージョンのバイナリーのフラグとチェックサムは、[公式 4.54 リリース](https://github.com/seaweedfs/seaweedfs/releases/tag/4.54)、[server コマンド](https://github.com/seaweedfs/seaweedfs/blob/4.54/weed/command/server.go)、[S3 設定ドキュメント](https://github.com/seaweedfs/seaweedfs/wiki/Amazon-S3-API)に従います。

## プロファイル移行と整合性監査 { #profile-migration-and-integrity-audit }

管理者 CLI は設定済みのデータベース接続を直接使用します。既存のスキーマを開くだけで、テーブルの作成、マイグレーションの実行、認証プロバイダーの初期化、ストレージサービスの起動は行いません。実行前に古いプロファイルと宛先のプロファイルを両方設定してください。データベースとオブジェクトの整合したバックアップを保持してください。

アップロードやデータベース変更を行わずに、選択した実行をプレビューします。

```sh
gpustack artifact-storage --config-file /etc/gpustack/config.yaml \
  migrate-profile --source-id monitoring-eu-v1 --target-id monitoring-eu-v2 \
  --benchmark-id 7 --run-id RUN_UUID --dry-run --manifest /secure/migration-plan.json
```

同じ選択を適用するには `--apply` を使用します。両方の操作フラグを省略した場合もドライランになります。`--archive-id`、`--benchmark-id`、`--run-id` で選択を絞り込めます。`--limit` は 1 回の呼び出しを制限します（既定は 100、最大は 1000 アーカイブ）。このコマンドは `ready`、`partial`、`no_data` 状態のアーカイブだけを処理し、有効なリースがあるアーカイブの取得は拒否します。Prometheus からデータを補完したり、未完了/失敗した収集を移行したりはしません。

アーカイブごとに、移行は既存のデータベースリースを取得してフェンシングトークンを増やし、アップロード前に予定するソース/宛先オブジェクト参照を永続化します。宛先プロファイルで安定したオブジェクトキーを再利用し、ソースと宛先の SHA-256 を検証し、オブジェクトごとに進捗をチェックポイントへ保存します。すべてのオブジェクトが検証を通過した後、1 つのフェンシングされたトランザクション内で、アーカイブおよび公開済みチャンクのプロファイル ID を変更します。スコープ ID、アーカイブリビジョン、タイムスタンプ、ペイロードのチェックサムは変わりません。そのため、公開済みリビジョンは同じデータを再生します。アーカイブは個別にコミットされるため、失敗したバッチにもすでに完了したアーカイブが含まれる場合があります。

中断後は同じコマンドを再実行してください。完了したアーカイブはソースプロファイルに一致しなくなり、未完了のコピーは永続化された進捗から再開します。リースを失うと公開はできなくなり、リース期限が切れた後（最大 3 分）に別のプロセスが再開できます。宛先コピーが見つからなければ、検証済みのソースから再作成します。コピーが破損または競合している場合は移行を停止し、管理者の調査を待ちます。移行の成功後も含め、ソースコピーは常にそのまま保持されます。

保持されたソースコピーと未完了の宛先コピーは、アーカイブの移行履歴に索引付きで残ります。通常の Benchmark 削除や設定済みのアーカイブ有効期限切れでは、同じ再試行可能な GC ジョブを通じてクリーンアップされます。クリーンアップが完了するまで、それらのストレージプロファイルを設定したままにしてください。暗黙にソースを即座に削除する処理や、バケット全体のクリーンアップコマンドはありません。一時的に二重となるストレージ容量を見込んでください。

ストレージやデータベース行を変更せずに、現在索引にあるオブジェクトを監査します。

```sh
gpustack artifact-storage --config-file /etc/gpustack/config.yaml \
  audit --profile-id monitoring-eu-v2 --manifest /secure/integrity-report.json
```

`check` は `audit` の別名です。同じアーカイブ/実行/Benchmark フィルターと件数制限が適用されます。レポートは欠落オブジェクト、チェックサム不一致、利用できないプロファイル、安全なストレージ障害理由を示し、ペイロードや認証情報は含みません。終了ステータスは、成功が 0、監査で問題を検出した場合が 2、操作失敗が 1 です。`--manifest` を指定すると、レポートを非公開の権限で書き込みます。監査は公開済みで索引にあるオブジェクトを確認し、バケット全体を走査したり、索引のないどのオブジェクトを削除できるか推測したりはしません。
