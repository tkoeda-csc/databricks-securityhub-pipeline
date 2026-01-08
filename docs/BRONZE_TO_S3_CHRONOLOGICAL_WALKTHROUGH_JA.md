# Bronze to S3 パイプライン - 時系列実行フロー

**ノートブック**: `bronze_to_s3.ipynb`
**目的**: AWS Security Hub 検出結果をブロンズテーブルから読み込み → 集計 → S3 へ CSV としてエクスポート
**日付**: 2026 年 1 月 5 日

---

## 実行フロー概要

```
1. セットアップと設定
   ↓
2. ユーティリティ関数の定義
   ↓
3. Security Hub Controls リファレンステーブルの読み込み
   ↓
4. 処理する会社の検出
   ↓
5. 各会社に対して:
   a. ブロンズデータの読み込み (ASFF + OCSF)
   b. 標準スキーマへの変換
   c. 検出結果の重複排除
   d. リファレンス重大度の適用
   e. コントロールレベルへの集計
   f. リージョナルサマリーへの集計
   g. アカウントサマリーへの集計
   h. S3 への書き込み
   ↓
6. 最終結果のレポート
```

---

## 1. セットアップと設定

### セル 1: 依存関係のインポート

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import traceback
from datetime import datetime
```

**目的**: PySpark と Python ライブラリのインポート

### セル 2: ジョブパラメータの取得

```python
dbutils.widgets.text("CATALOG_NAME", "")
dbutils.widgets.text("COMPANY_INDEX_ID", "")

catalog_name = dbutils.widgets.get("CATALOG_NAME").strip()
company_index_id_param = dbutils.widgets.get("COMPANY_INDEX_ID").strip()

if not catalog_name:
    raise ValueError("Missing required param: CATALOG_NAME")
```

**実行内容**: Databricks ウィジェットからジョブパラメータを読み込む
**必須**: `CATALOG_NAME` (Unity Catalog 名)
**オプション**: `COMPANY_INDEX_ID` (特定の会社または "ALL")

### セル 3: Spark の設定

```python
spark.conf.set("spark.sql.session.timeZone", "UTC")
```

**実行内容**: タイムゾーンを UTC に設定してタイムスタンプの一貫性を確保

### セル 4: 処理ウィンドウの定義

```python
job_date = F.date_trunc("DAY", F.current_timestamp())
window_end_ts = job_date
window_start_ts = window_end_ts - F.expr("INTERVAL 48 HOURS")
```

**実行内容**: 48 時間のタイムウィンドウを計算
**ウィンドウ**: `[今日 - 48時間, 今日 00:00:00]`
**48 時間の理由**: Security Hub チェックは 18 時間ごとに実行されるため、48 時間で完全なカバレッジを確保

### セル 5: S3 設定

```python
s3_base_path = "s3://dev-cf-databricks-catalog-bucket/dev/dashboard"
date_str = datetime.now().strftime('%Y-%m-%d')
is_all_companies = not company_index_id_param or company_index_id_param.upper() == "ALL"
```

**実行内容**: S3 出力先と処理モードの定義
**出力パス構造**: `s3://bucket/company_id/aws/data_type/YYYY-MM-DD/`

---

## 2. ユーティリティ関数の定義

**タイミング**: ノートブック開始時に実行（関数定義）
**後で使用**: 会社の検出と処理中

### 関数: `is_valid_company_id(schema_name: str) -> bool`

```python
def is_valid_company_id(schema_name: str) -> bool:
    return (
        len(schema_name) == 12 and
        schema_name.isalnum() and
        schema_name.islower()
    )
```

**目的**: 会社 ID 形式の検証（12 文字、小文字英数字）

### 関数: `table_exists(full_name: str) -> bool`

```python
def table_exists(full_name: str) -> bool:
    try:
        return spark.catalog.tableExists(full_name)
    except Exception:
        return False
```

**目的**: カタログ内のテーブルの存在確認

### 関数: `discover_companies(catalog: str) -> list`

```python
def discover_companies(catalog: str) -> list:
    spark.sql(f"USE CATALOG {catalog}")
    schemas = spark.catalog.listDatabases()

    companies = []
    for schema in schemas:
        schema_name = schema.name

        if '.' in schema_name:
            parts = schema_name.split('.')
            if parts[0] == catalog and len(parts) == 2:
                schema_name = parts[1]
            else:
                continue

        if is_valid_company_id(schema_name):
            companies.append(schema_name)

    return sorted(companies)
```

**目的**: カタログ内の有効な会社スキーマの検出

### 関数: `normalize_finding_id(col)`

```python
def normalize_finding_id(col):
    return F.when(F.length(F.trim(col)) == 0, F.lit(None)).otherwise(F.trim(col))

def parse_iso8601_to_ts(col):
    return F.to_timestamp(col)
```

**目的**: フィールド正規化ヘルパー

### 関数: `transform_asff(df)` と `transform_ocsf(df)`

```python
def transform_asff(df):
    return (
        df
        .where(F.col("RecordState") != "ARCHIVED")
        .select(
            normalize_finding_id(F.col("finding_id")).alias("finding_id"),
            parse_iso8601_to_ts(F.col("updated_at")).alias("finding_modified_time"),
            F.upper(F.col("workflow.Status")).alias("finding_status"),
            F.col("aws_account_id").cast("string").alias("account_id"),
            F.col("finding_region").cast("string").alias("region_id"),
            F.expr("compliance.AssociatedStandards[0].StandardsId").cast("string").alias("standard_id"),
            F.col("compliance.SecurityControlId").cast("string").alias("control_id"),
            F.col("compliance.Status").cast("string").alias("compliance_status"),
            F.col("severity.Label").cast("string").alias("severity"),
            F.col("cf_processed_time").alias("_bronze_processed_time"),
            F.lit(1).alias("_preference")
        )
    )
```

**目的**: ASFF/OCSF を標準スキーマに変換
**主要なアクション**: ARCHIVED をフィルタリング、フィールドの正規化、優先度フラグの追加

### 関数: `aggregate_findings_to_controls(findings_df)`

```python
def aggregate_findings_to_controls(findings_df):
    findings = (
        findings_df
        .withColumn("compliance_status", F.upper("compliance_status"))
        .withColumn("severity", F.when(F.col("severity").isNull(), "unclassified").otherwise(F.lower("severity")))
        .withColumn("is_suppressed", F.upper(F.col("finding_status")) == F.lit("SUPPRESSED"))
        .withColumn("severity_rank", F.when(F.col("severity") == "critical", 4).when(...))
    )

    control_key = ["account_id", "region_id", "standard_id", "control_id"]

    controls = (
        findings
        .groupBy(*control_key)
        .agg(
            F.sum(F.when(~F.col("is_suppressed"), 1).otherwise(0)).alias("active_cnt"),
            F.sum(F.when((~F.col("is_suppressed")) & (F.col("compliance_status") == "FAILED"), 1).otherwise(0)).alias("failed_cnt"),
            F.sum(F.when((~F.col("is_suppressed")) & (F.col("compliance_status") == "PASSED"), 1).otherwise(0)).alias("passed_cnt"),
            F.sum(F.when((~F.col("is_suppressed")) & (F.col("compliance_status").isin("WARNING", "NOT_AVAILABLE")), 1).otherwise(0)).alias("unknown_cnt"),
            F.count("*").alias("total_cnt"),
            F.max("severity_rank").alias("max_severity_rank")
        )
        .withColumn("control_status", F.when(...))
        .withColumn("severity", F.when(...))
        .drop("max_severity_rank")
    )

    return controls
```

**目的**: 検出結果をコントロールレベルのメトリクスに集計
**出力**: (account, region, standard, control) ごとに 1 行、ステータス付き

### 関数: `aggregate_account_region_summary(controls_df, company_id)`

```python
def aggregate_account_region_summary(controls_df, company_id):
    # 重大度レベルの集計...
    # 標準レベルの集計...
    # リージョナルサマリー...
    # 結合して最終 DataFrame を返す
```

**目的**: 重大度の内訳を含むリージョナル標準サマリーの作成

---

## 3. Security Hub Controls リファレンステーブルの読み込み

**タイミング**: 会社の処理前

```python
controls_ref_table = f"{catalog_name}.reference.securityhub_controls"

try:
    if table_exists(controls_ref_table):
        controls_ref_df = spark.table(controls_ref_table).select("control_id", "severity")
        ref_count = controls_ref_df.count()
    else:
        controls_ref_df = None
except Exception as e:
    controls_ref_df = None
```

**実行内容**:

1. リファレンステーブルの存在確認
2. 存在する場合: control_id → 重大度マッピングの読み込み
3. 存在しない場合: None に設定（ソースデータの重大度を使用）

**後で使用**: 処理中に正しい値で検出結果の重大度を上書き

---

## 4. 処理する会社の検出

**タイミング**: メインループ前

```python
if not company_index_id_param or company_index_id_param.upper() == "ALL":
    companies_to_process = discover_companies(catalog_name)
else:
    if not is_valid_company_id(company_index_id_param):
        raise ValueError(f"Invalid company_id format")
    companies_to_process = [company_index_id_param]
```

**実行内容**:

-   **モード 1 (ALL)**: カタログ内のすべての有効な会社を自動検出
-   **モード 2 (単一)**: 特定の会社 1 つを検証して処理

**結果**: 処理する会社 ID のリスト

---

## 5. パイプライン実行ループ

**タイミング**: メイン実行開始

```python
successful_companies = []
failed_companies = []
skipped_companies = []
total_stats = {"standards_rows": 0, "account_rows": 0}

for company_id in companies_to_process:
    success, message, stats = process_company(
        company_id, catalog_name, window_start_ts, window_end_ts, s3_base_path, date_str
    )

    if success:
        successful_companies.append(company_id)
        total_stats["standards_rows"] += stats.get("standards_rows", 0)
        total_stats["account_rows"] += stats.get("account_rows", 0)
    elif message in ["No bronze tables", "No data in window", "No valid findings", "No summary data"]:
        skipped_companies.append((company_id, message))
    else:
        failed_companies.append((company_id, message))
```

**実行内容**: 各会社をループ処理し、結果を追跡

---

## 6. 会社ごとの処理

**関数**: `process_company(company_id, ...)`

### ステップ 6a: テーブル名の定義

```python
asff_tbl = f"{catalog_name}.{company_id}.aws_securityhub_findings_1_0"
ocsf_tbl = f"{catalog_name}.{company_id}.aws_securitylake_sh_findings_2_0"
```

**実行内容**: 完全修飾ブロンズテーブル名の構築

### ステップ 6b: テーブル存在確認

```python
asff_exists = table_exists(asff_tbl)
ocsf_exists = table_exists(ocsf_tbl)

if not asff_exists and not ocsf_exists:
    return (False, "No bronze tables", {})
```

**実行内容**: 少なくとも 1 つのソースが存在することを確認
**早期終了**: テーブルが見つからない場合は会社をスキップ

### ステップ 6c: ブロンズデータの読み込み

```python
sources = []

if asff_exists:
    df_asff_raw = (
        spark.table(asff_tbl)
        .where(
            (F.col("product_name") == "Security Hub") &
            (F.col("cf_processed_time") >= window_start_ts) &
            (F.col("cf_processed_time") < window_end_ts)
        )
    )
    asff_count = df_asff_raw.count()
    if asff_count > 0:
        sources.append(("ASFF", df_asff_raw))

if ocsf_exists:
    df_ocsf_raw = (
        spark.table(ocsf_tbl)
        .where(
            (F.col("metadata.product.name") == "Security Hub") &
            (F.col("cf_processed_time") >= window_start_ts) &
            (F.col("cf_processed_time") < window_end_ts)
        )
    )
    ocsf_count = df_ocsf_raw.count()
    if ocsf_count > 0:
        sources.append(("OCSF", df_ocsf_raw))

if len(sources) == 0:
    return (False, "No data in window", {})
```

**実行内容**:

1. 48 時間ウィンドウでフィルタリングされた ASFF テーブルの読み込み
2. 48 時間ウィンドウでフィルタリングされた OCSF テーブルの読み込み
3. データがあるソースのみを保持
4. データが見つからない場合は早期終了

### ステップ 6d: 標準スキーマへの変換

```python
canonical_dfs = []
for src, df_raw in sources:
    if src == "ASFF":
        out = transform_asff(df_raw)
    elif src == "OCSF":
        out = transform_ocsf(df_raw)
    else:
        continue

    out = out.withColumn("finding_id", normalize_finding_id(F.col("finding_id"))) \
             .where(F.col("finding_id").isNotNull())
    canonical_dfs.append(out)

if not canonical_dfs:
    return (False, "No valid findings", {})

df_union = canonical_dfs[0]
for d in canonical_dfs[1:]:
    df_union = df_union.unionByName(d, allowMissingColumns=True)
```

**実行内容**:

1. 各ソース（ASFF/OCSF）を標準スキーマに変換
2. finding_id の正規化（トリム、NULL フィルタリング）
3. すべてのソースを単一の DataFrame に結合

### ステップ 6e: 検出結果の重複排除

```python
w = Window.partitionBy("finding_id").orderBy(
    F.col("finding_modified_time").desc_nulls_last(),
    F.col("_preference").desc(),
    F.col("_bronze_processed_time").desc_nulls_last()
)

findings = (
    df_union
    .withColumn("_rn", F.row_number().over(w))
    .where(F.col("_rn") == 1)
    .drop("_rn", "_preference", "_bronze_processed_time")
)
```

**実行内容**:

1. `finding_id` でパーティション分割（各ユニークな検出結果をグループ化）
2. 各グループ内で順序付け:
    - **優先度 1**: 最新の更新時刻
    - **優先度 2**: ASFF を OCSF より優先
    - **優先度 3**: 最新のバッチ
3. 行番号を割り当て（1=最良、2=次点、...）
4. 行番号 1 のみを保持（勝者）
5. ヘルパーカラムを削除

**結果**: finding_id ごとに 1 つの標準レコード

### ステップ 6f: リファレンステーブルの重大度の適用

```python
if controls_ref_df is not None:
    findings = (
        findings
        .join(
            controls_ref_df.withColumnRenamed("severity", "ref_severity"),
            on="control_id",
            how="left"
        )
        .withColumn(
            "severity",
            F.when(F.col("ref_severity").isNotNull(), F.lower(F.col("ref_severity")))
             .otherwise(F.lower(F.col("severity")))
        )
        .drop("ref_severity")
    )
```

**実行内容**:

1. `control_id` で検出結果とリファレンステーブルを左結合
2. リファレンスの重大度が存在する場合 → それを使用
3. それ以外の場合 → 検出結果の重大度を保持

### ステップ 6g: コントロールレベルへの集計

```python
controls = aggregate_findings_to_controls(findings)
```

**実行内容**:

1. (account, region, standard, control) で検出結果をグループ化
2. メトリクスの計算: active_cnt, failed_cnt, passed_cnt, unknown_cnt
3. control_status の決定（PASSED/FAILED/UNKNOWN/NO_DATA）
4. 重大度の割り当て（すべての検出結果から最高のもの）

**結果**: 各コントロール（各アカウント・リージョン・標準・コントロールの組み合わせ）ごとに 1 行のデータ、集計されたステータスと統計情報を含む

**具体例**:

-   入力: 同じコントロール（例: IAM.1）に対する 3 つの検出結果
    -   検出結果 1: ステータス=FAILED
    -   検出結果 2: ステータス=FAILED
    -   検出結果 3: ステータス=PASSED
-   出力: IAM.1 コントロールに対して 1 行だけ
    -   control_status = FAILED（1 つでも失敗があるため）
    -   failed_cnt = 2
    -   passed_cnt = 1
    -   active_cnt = 3

### ステップ 6h: リージョナルサマリーへの集計

```python
standards_summary_df = aggregate_account_region_summary(controls, company_id)
```

**実行内容**:

1. **重大度の集計**: 重大度レベルごとのコントロール数をカウント
2. **標準の集計**: 標準ごとにグループ化、重大度内訳を収集
3. **リージョナルの集計**: 合計ルール数をカウント、全体スコアを計算
4. **結合**: リージョナルメトリクスと標準配列を結合
5. **メタデータの追加**: company_id, cf_processed_time

**結果**: standards_summary 配列を持つ (company, account, region) ごとに 1 行

### ステップ 6i: Standards Summary の S3 への書き込み

```python
standards_summary_csv = standards_summary_df.withColumn(
    "standards_summary",
    F.to_json("standards_summary")
)

standards_s3_path = f"{s3_base_path}/{company_id}/aws/standards_summary/{date_str}"

(standards_summary_csv
 .coalesce(1)
 .write
 .mode("overwrite")
 .option("header", "true")
 .option("compression", "gzip")
 .option("quote", '"')
 .option("escape", '"')
 .csv(standards_s3_path))
```

**実行内容**:

1. `standards_summary` 配列を JSON 文字列に変換
2. 単一の CSV ファイルを書き込み（coalesce(1)）
3. gzip 圧縮を有効化
4. 適切な CSV クォーティングを設定

**出力**: `s3://bucket/company_id/aws/standards_summary/2026-01-05/part-*.csv.gz`

### ステップ 6j: アカウントサマリーへの集計

```python
account_summary = (
    standards_summary_df
    .groupBy("company_id", "account_id")
    .agg(
        F.sum("total_rules").cast("int").alias("total_rules"),
        F.sum("total_passed").cast("int").alias("total_passed"),
        F.first("cf_processed_time").alias("cf_processed_time")
    )
    .withColumn(
        "score",
        F.round(
            F.when(F.col("total_rules") > 0, (F.col("total_passed") / F.col("total_rules")) * 100)
             .otherwise(0.0),
            7
        )
    )
    .select("company_id", "cf_processed_time", "account_id", "score", "total_rules", "total_passed")
)
```

**実行内容**:

1. (company, account) でグループ化 - すべてのリージョンを合計
2. 全体的なアカウントコンプライアンススコアを計算
3. スコアを小数点以下 7 桁に丸める

**結果**: 集計されたメトリクスを持つアカウントごとに 1 行

### ステップ 6k: Account Summary の S3 への書き込み

```python
account_s3_path = f"{s3_base_path}/{company_id}/aws/account_compliance_summary/{date_str}"

(account_summary
 .coalesce(1)
 .write
 .mode("overwrite")
 .option("header", "true")
 .option("compression", "gzip")
 .csv(account_s3_path))
```

**実行内容**: アカウントレベルの CSV を S3 に書き込み

**出力**: `s3://bucket/company_id/aws/account_compliance_summary/2026-01-05/part-*.csv.gz`

### ステップ 6l: 成功を返す

```python
return (True, "Success", {
    "standards_rows": standards_summary_count,
    "account_rows": account_count
})
```

**実行内容**: 行数とともに成功ステータスを返す

---

## 7. 最終サマリー

```python
if failed_companies:
    raise Exception(f"Pipeline failed for {len(failed_companies)} company(ies).")
```

**実行内容**:

-   成功/スキップ/失敗した会社のサマリーを出力
-   予期しない失敗があった会社がある場合は例外を発生
-   すべての会社が正常に処理されたかスキップされた場合のみジョブが成功

---

## 完全な実行タイムライン

```
時刻 0: ノートブック開始
  ├─ ライブラリのインポート
  ├─ ジョブパラメータの取得
  ├─ Spark の設定（UTC タイムゾーン）
  ├─ タイムウィンドウの計算（48時間）
  └─ ユーティリティ関数の定義

時刻 1: リファレンスデータの読み込み
  └─ コントロールリファレンステーブルの読み込み（control_id → severity）

時刻 2: 会社の検出
  └─ 有効な会社スキーマのカタログクエリ

時刻 3: メインループ - 各会社に対して:
  │
  ├─ 会社 1 (例: abc123456789)
  │   ├─ テーブル存在確認
  │   ├─ ASFF の読み込み（存在する場合、タイムウィンドウ内）
  │   ├─ OCSF の読み込み（存在する場合、タイムウィンドウ内）
  │   ├─ ASFF → 標準への変換
  │   ├─ OCSF → 標準への変換
  │   ├─ ASFF + OCSF の結合
  │   ├─ finding_id による重複排除
  │   ├─ リファレンステーブルから重大度を上書き
  │   ├─ コントロールレベルへの集計
  │   ├─ リージョナルサマリーへの集計
  │   ├─ standards_summary を S3 に書き込み
  │   ├─ アカウントレベルへの集計
  │   └─ account_summary を S3 に書き込み
  │
  ├─ 会社 2 (例: def987654321)
  │   └─ （同じステップ...）
  │
  └─ 会社 N

時刻 4: 最終レポート
  ├─ サマリー統計を出力
  ├─ 成功した会社をリスト
  ├─ スキップした会社をリスト
  ├─ 失敗した会社をリスト
  └─ 失敗がある場合は例外を発生

終了: ジョブ完了
```

---

## 主要な時系列的洞察

### 遅延評価

PySpark は遅延評価を使用 - 変換は記録されますが、アクション（`.count()` や `.write()` など）が呼び出されるまで実行されません。

**例**: 次のように書いた場合:

```python
df_transformed = df.withColumn("new_col", ...)
```

まだ何も起こりません！変換は記録されますが実行されていません。

**実行トリガー**（アクション）:

-   `.count()`
-   `.show()`
-   `.write.csv()`
-   `.collect()`

### 会社内での実行順序

1. **読み込みとカウント**: Spark がブロンズテーブルを読み込むトリガー
2. **変換**: まだ遅延（実行なし）
3. **重複排除**: ウィンドウ関数を追加（まだ遅延）
4. **集計**: グループ化を記録（まだ遅延）
5. **S3 への書き込み**: **⚡ ここで実行される** - Spark が最適化して DAG 全体を実行

### 並列 vs 順次

-   **会社**: **順次**処理（一度に 1 つ）
-   **会社内**: Spark がデータ処理を内部的に並列化
-   **ASFF + OCSF の読み込み**: 順次実行、その後結合

---

## まとめ

ノートブックは次の正確な順序で実行されます:

1. セットアップと設定
2. 関数定義（まだ実行されていない）
3. リファレンステーブルの読み込み
4. 会社の検出
5. 会社のループ処理（順次）
6. 各会社に対して:
    - 読み込み → 変換 → 重複排除 → 集計 → 書き込み
7. 結果のレポート

各会社は次の会社に移る前に完全に処理されます（読み込みから書き込みまで）。
