# Combined Confluence Documentation - Part 1

*Generated from Confluence export - Part 1 of the collection*

## CF/1179025411.md

1. [CloudFastener開発プロジェクト](index.html)



#  CloudFastener開発プロジェクト : Databricks Job (Bronze→Gold/S3)

Created by  小枝泰之助, last modified on Dec 25, 2025

## 概要

### パイプラインの目的

AWS Security Hubの生のセキュリティFindingsをコンプライアンスメトリクスに変換し、結果をS3に保存して外部システムで利用可能にする。

### 出力

  1. **リージョン別サマリー** (`aws_standard_summary`) - AWSリージョンごとのコンプライアンス状況

  2. **アカウント別サマリー** (`aws_account_compliance_summary`) - AWSアカウントごとのコンプライアンス状況




* * *

## データフロー概要


    ┌──────────────────────────────────────────────────────────────────┐
    │  BRONZE レイヤー (Databricks Delta テーブル)                        │
    │  - Security Hub の生Findings                                      │
    │  - AWS Security Hub 更新スケジュール:                               │
    │    • 定期チェック: 12時間または24時間ごと                             │
    │    • 変更トリガー: リソース設定が変更されたとき or 最大18時間ごと         │　
    │  - Databricks Ingester Job頻度：1日1回                             │
    │  - 2つのフォーマット: ASFF (ネイティブ) と OCSF (Security Lake)       │
    └──────────────────────────────────────────────────────────────────┘
                                ↓
                  [ Databricksで処理 (日次ジョブ) ]
                                ↓
    ┌──────────────────────────────────────────────────────────────────┐
    │  変換処理 (7ステップ)                                               │
    │  1. 48時間分のデータウィンドウを抽出                                  │
    │  2. ASFF & OCSF を共通スキーマに正規化                               │
    │  3. Findingsの重複排除                                             │
    │  4. 参照テーブルを使用してSeverityを修正　　　　　　　　　　　　　　　　　　│
    │  5. コントロールレベルに集約　　　　　　　　　　　　　　　　　　　　　　　　　│
    │  6. リージョン別サマリーを作成                                        │
    │  7. アカウント別サマリーを作成                                        │
    └──────────────────────────────────────────────────────────────────┘
                                ↓
    ┌──────────────────────────────────────────────────────────────────┐
    │  S3 エクスポート                                          │
    │  s3://cf-databricks-compliance-data/                             │
    │    ├── aws_standard_summary/                                     │
    │    │   └── company_id=xxx/date=2024-12-24/                       │
    │    │       └── data.csv                                          │
    │    └── aws_account_compliance_summary/                           │
    │        └── company_id=xxx/date=2024-12-24/                       │
    │            └── data.csv                                          │
    └──────────────────────────────────────────────────────────────────┘
                                ↓
              　　   [ Aurora MySQL, ダッシュボード]


## ジョブ実行モデル

### ジョブの動作

ジョブは**1日1回** 実行され、**すべての企業を順次** 処理します。このセクションでは、実行フロー、企業の検出、エラーハンドリングについて説明します。

### ジョブパラメータ

**このコードが行うこと:**
処理する企業を制御するジョブパラメータを定義します。ジョブ実行時に、特定の企業を指定するか、すべての企業を自動的に処理できます。

**コード参照:**


    dbutils.widgets.text("CATALOG_NAME", "")  # Unity Catalog名
    dbutils.widgets.text("COMPANY_INDEX_ID", "")  # 特定の企業または空白ですべて

    catalog_name = dbutils.widgets.get("CATALOG_NAME")
    company_index_id_param = dbutils.widgets.get("COMPANY_INDEX_ID")

**パラメータ:**

パラメータ| 説明| 例| デフォルト
---|---|---|---
`CATALOG_NAME`| Unity Catalog名| `cloudfastener`| 必須
`COMPANY_INDEX_ID`| 特定の企業またはすべて| `xs22xw4aw73q` | 空白 (= すべて)

**実行モード:**

  1. **すべての企業 (デフォルト):** カタログ内のすべての企業を処理

  2. **単一企業:** 指定された企業のみを処理




### 企業の検出

**このコードが行うこと:**
有効な企業IDフォーマット(小文字英数字12文字)のスキーマをスキャンして、カタログ内のすべての企業スキーマを自動的に検出します。これにより企業リストを手動で管理する必要がなくなります。

**コード参照:**


    def discover_companies(catalog: str) -> list:
        """カタログ内のすべての企業スキーマを検出"""
        databases = spark.catalog.listDatabases()
        companies = []

        for db in databases:
            # フルパスからスキーマ名を抽出
            parts = db.name.split('.')
            if len(parts) >= 2 and parts[0] == catalog:
                schema_name = parts[1]
            else:
                schema_name = parts[0]

            # フォーマットを検証: 小文字英数字12文字
            if is_valid_company_id(schema_name):
                companies.append(schema_name)

        return sorted(companies)


**企業IDフォーマット検証:**


    def is_valid_company_id(schema_name: str) -> bool:
        """スキーマ名が企業IDフォーマットに一致するかチェック"""
        return (
            len(schema_name) == 12 and      # 正確に12文字
            schema_name.isalnum() and        # 英数字のみ
            schema_name.islower()            # 小文字のみ
        )


**検出の例:**


    Catalog: cloudfastener
    ├─ reference (スキーマ) → スキップ (企業ではない)
    ├─ xs22xw4aw73q (スキーマ) → ✓ 有効な企業
    ├─ ab12cd34ef56 (スキーマ) → ✓ 有効な企業
    ├─ test_company (スキーマ) → スキップ (無効なフォーマット)
    └─ PROD123 (スキーマ) → スキップ (大文字)

    結果: 2社を検出 [xs22xw4aw73q, ab12cd34ef56]

### パラレルバッチ処理ループ

**このコードが行うこと:**
各企業からDataFrameを収集し、すべての企業のデータを単一のバッチ書き込みで並列処理します。これにより、10社の場合は10倍の高速化（10分→1分）を実現します。

**コード参照:**


    # 結果とDataFrameを収集
    successful_companies = []
    failed_companies = []
    skipped_companies = []

    standards_dfs = []  # 標準サマリーDataFrame
    account_dfs = []    # アカウントサマリーDataFrame

    # 各企業を処理してDataFrameを収集
    for company_id in companies_to_process:
        standards_df, account_df, (success, message) = process_company(
            company_id,
            catalog_name,
            window_start_ts,
            window_end_ts,
            cf_processed_time
        )

        # 結果を分類してDataFrameを収集
        if success:
            successful_companies.append(company_id)
            if standards_df is not None:
                standards_dfs.append(standards_df)
            if account_df is not None:
                account_dfs.append(account_df)
        elif message in ["No bronze tables", "No data in window", "No valid findings", "No summary data"]:
            skipped_companies.append((company_id, message))
        else:
            failed_companies.append((company_id, message))

    # ============================================================
    # S3へのバッチ書き込み（並列処理）
    # ============================================================

    if standards_dfs:
        # すべての標準サマリーDataFrameを結合
        combined_standards = standards_dfs[0]
        for df in standards_dfs[1:]:
            combined_standards = combined_standards.unionByName(df, allowMissingColumns=True)

        # 複合型をCSV互換のJSON文字列に変換
        # CSVはarray/struct型をサポートしないため、`standards_summary`をJSON文字列に変換
        # Aurora MySQLでのインポート時にJSONとして解析可能
        combined_standards_csv = combined_standards.withColumn(
            "standards_summary",
            F.to_json("standards_summary")
        )

        # company_idで再パーティション化して並列書き込み
        # 10社の場合：10個のSparkタスクが同時にS3に書き込む
        (combined_standards_csv
         .repartition("company_id")  # 企業ごとに並列書き込み、100社以上にスケーラブル
         .write
         .mode("overwrite")
         .option("header", "true")
         .option("compression", "gzip")
         .option("maxRecordsPerFile", 200000)
         .partitionBy("company_id", "date")
         .csv(standards_s3_path))

    if account_dfs:
        # アカウントサマリーも同様に処理
        combined_accounts = account_dfs[0]
        for df in account_dfs[1:]:
            combined_accounts = combined_accounts.unionByName(df, allowMissingColumns=True)

        (combined_accounts
         .repartition("company_id")
         .write
         .mode("overwrite")
         .option("header", "true")
         .option("compression", "gzip")
         .option("maxRecordsPerFile", 200000)
         .partitionBy("company_id", "date")
         .csv(account_s3_path))

**重要ポイント:**

1. **ループは必要**: 各企業は異なるブロンズテーブルを持つため、個別にクエリする必要がある
2. **ループは高速**: S3書き込みがないため、メモリ内のSpark操作のみ
3. **並列性は書き込みで発生**: `.repartition("company_id")` が10社を10個のSparkタスクに分散

* * *

## 企業ごとの処理: 詳細処理ステップ

**process_company() 関数:**

各企業は同じ7ステップのパイプラインを通過し、最終的にDataFrameを返します（S3には書き込まず）:

### ステップ1: データ選択 (48時間ウィンドウ)

**このコードが行うこと:**
BronzeテーブルからSecurity Hubの生Findingsをロードし、過去48時間以内のFindingsのみをフィルタリングし、アーカイブされたFindingsを除外します。これにより最新のアクティブなFindingsを処理します。

**ソーステーブル:**

  * `cloudfastener.{company_id}.aws_securityhub_findings_1_0` (ASFF形式)

  * `cloudfastener.{company_id}.aws_securitylake_sh_findings_2_0` (OCSF形式)




**コード参照:**


    # ファイル: bronze_to_gold_v2.ipynb, 行 390-405
    df_asff_raw = (
        spark.table(asff_tbl)
        .where(
            (F.col("product_name") == "Security Hub") &
            (F.col("cf_processed_time") >= window_start_ts) &  # 過去48時間
            (F.col("cf_processed_time") < window_end_ts) &
            (F.col("RecordState") != "ARCHIVED")  # アーカイブ済みを除外
        )
    )


* * *

### ステップ2: スキーマ正規化

**このコードが行うこと:**
ASFFとOCSFの両方の形式を単一の統一スキーマに変換します。これにより両ソースからのFindingsを一緒に処理できます。また、ワークフローステータス値 (NEW, NOTIFIED, SUPPRESSED, RESOLVED) をASFF語彙に標準化します。

**目的:** ASFFとOCSFの両方を統一スキーマに変換

**コード参照:**


    def transform_asff(df):
        """ASFF → 正規化スキーマ"""
        return df.select(
            normalize_finding_id(F.col("finding_id")),
            parse_iso8601_to_ts(F.col("updated_at")).alias("finding_modified_time"),
            F.upper(F.col("workflow.Status")).alias("finding_status"),  # NEW, NOTIFIED, SUPPRESSED, RESOLVED
            F.col("aws_account_id").cast("string").alias("account_id"),
            F.col("finding_region").cast("string").alias("region_id"),
            F.col("compliance.SecurityControlId").alias("control_id"),
            F.col("compliance.Status").alias("compliance_status"),
            F.col("severity.Label").alias("severity")
        )

    def transform_ocsf(df):
        """OCSF → 同じ正規化スキーマ"""
        return df.select(
            normalize_finding_id(F.col("finding_info.uid")),
            parse_iso8601_to_ts(F.col("finding_info.modified_time_dt")),
            # 一貫性のためASFF語彙にマップ
            F.when(F.upper(status) == "IN_PROGRESS", "NOTIFIED")
             .otherwise(F.upper(status)).alias("finding_status"),
            F.col("cloud.account.uid").alias("account_id"),
            F.col("cloud.region").alias("region_id"),
            F.col("compliance.control").alias("control_id"),
            F.col("compliance.status").alias("compliance_status"),
            F.col("severity")
        )


**抽出される主要フィールド:**

フィールド| 説明| 例
---|---|---
`finding_id`| 一意のFinding識別子| "arn:aws:securityhub:..."
`finding_modified_time`| Findingが最後に更新された日時| "2024-12-24T10:30:00Z"
`finding_status`| ワークフローステータス| NEW, NOTIFIED, SUPPRESSED, RESOLVED
`account_id`| AWSアカウントID| "123456789012"
`region_id`| AWSリージョン| "us-east-1"
`control_id`| セキュリティコントロール| "S3.1", "IAM.1"
`compliance_status`| コンプライアンス状態| PASSED, FAILED, WARNING
`severity`| リスクレベル| CRITICAL, HIGH, MEDIUM, LOW

* * *

### ステップ3: 重複排除

**問題:** 同じFindingがASFFとOCSFの両方のテーブルに存在する可能性

**コード参照:**


    # ASFFとOCSFのデータを結合
    df_union = df_asff.unionByName(df_ocsf)

    # 重複排除: finding_idごとに1つのFindingを保持
    w = Window.partitionBy("finding_id").orderBy(
        F.col("finding_modified_time").desc_nulls_last(),  # 1. 最新の更新を優先
        F.col("_preference").desc(),                        # 2. ASFFをOCSFより優先
        F.col("_bronze_processed_time").desc_nulls_last()  # 3. より新しいバッチを優先
    )

    findings = (
        df_union
        .withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)  # finding_idごとに最初の行を保持
        .drop("_rn", "_preference", "_bronze_processed_time")
    )

**なぜASFFが優先されるのか?**

  * ASFFはAWS Security Hubのネイティブ形式

  * より信頼性が高く詳細

  * 企業間で一貫性を確保




* * *

### ステップ4: Severity修正 (新機能)

**このコードが行うこと:**
Findingsを各コントロールの正しいSeverityレベルを含む参照テーブルと結合します。

FindingのSeverityが参照テーブルと一致しない場合は修正されます。

これにより、同じコントロールのすべてのFindingsが一貫した正確なSeverityレベルを持つことが保証されます。

**問題:** 一貫性のないSeverityレベルがある可能性

**解決策:** 参照テーブルと結合

**参照テーブル:** `cloudfastener.reference.securityhub_controls`

  * コントロール数：560




**コード参照:**


    # 参照テーブルをロード
    controls_ref_df = spark.table("cloudfastener.reference.securityhub_controls")
    # 含まれる内容: control_id, severity

    # Findingsを参照テーブルと結合
    findings = (
        findings
        .join(
            controls_ref_df.withColumnRenamed("severity", "ref_severity"),
            on="control_id",
            how="left"  # すべてのFindingsを保持
        )
        .withColumn(
            "severity",
            # 参照Severityが利用可能な場合は使用、そうでない場合は元のままにする
            F.when(F.col("ref_severity").isNotNull(), F.lower(F.col("ref_severity")))
             .otherwise(F.lower(F.col("severity")))
        )
        .drop("ref_severity")
    )


**これが重要な理由:**

  * 正確なリスク評価を保証

  * すべてのFindings間でSeverityの一貫性

  * コンプライアンススコアリングに不可欠




* * *

### ステップ5: コントロールレベルの集約

**このコードが行うこと:**
個々のFindingsをcontrol_idでグループ化し、全体的なコントロールステータスを決定します。

各コントロールのPASSED対FAILEDのFindings数をカウントします。

いずれかのFindingがFAILEDの場合、コントロール全体がFAILEDとマークされます。

抑制されたFindingsは別途カウントされますが、合格/不合格ステータスには影響しません。

**目的:** 個々のFindingsをコントロールコンプライアンスステータスにロールアップ

**コード参照:**

![](images/icons/grey_arrow_down.png) def aggregate_findings_to_controls


    def aggregate_findings_to_controls(findings_df):
        """
        Aggregate findings to control-level status.
        AWS Security Hub CSPM-compliant aggregation logic.
        """
        # Normalize compliance status and severity
        findings = (
            findings_df
            .withColumn("compliance_status", F.upper("compliance_status"))
            .withColumn(
                "severity",
                F.when(F.col("severity").isNull(), "unclassified")
                 .otherwise(F.lower("severity"))
            )
            .withColumn(
                "is_suppressed",
                F.upper(F.col("finding_status")) == F.lit("SUPPRESSED")
            )
            .withColumn(
                "severity_rank",
                F.when(F.col("severity") == "critical", 4)
                 .when(F.col("severity") == "high", 3)
                 .when(F.col("severity") == "medium", 2)
                 .when(F.col("severity") == "low", 1)
                 .otherwise(0)
            )
        )

        # Control-level aggregation
        control_key = ["account_id", "region_id", "standard_id", "control_id"]

        controls = (
            findings
            .groupBy(*control_key)
            .agg(
                # Count-based aggregation (CSPM-compliant)
                F.sum(F.when(~F.col("is_suppressed"), 1).otherwise(0)).alias("active_cnt"),
                F.sum(F.when((~F.col("is_suppressed")) & (F.col("compliance_status") == "FAILED"), 1).otherwise(0)).alias("failed_cnt"),
                F.sum(F.when((~F.col("is_suppressed")) & (F.col("compliance_status") == "PASSED"), 1).otherwise(0)).alias("passed_cnt"),
                F.sum(F.when((~F.col("is_suppressed")) & (F.col("compliance_status").isin("WARNING", "NOT_AVAILABLE")), 1).otherwise(0)).alias("unknown_cnt"),
                F.count("*").alias("total_cnt"),
                F.max("severity_rank").alias("max_severity_rank")
            )
            .withColumn(
                "control_status",
                F.when(F.col("active_cnt") == 0, "NO_DATA")
                 .when(F.col("failed_cnt") > 0, "FAILED")
                 .when(F.col("unknown_cnt") > 0, "UNKNOWN")
                 .when(F.col("passed_cnt") == F.col("active_cnt"), "PASSED")
                 .otherwise("UNKNOWN")
            )
            .withColumn(
                "severity",
                F.when(F.col("max_severity_rank") == 4, "critical")
                 .when(F.col("max_severity_rank") == 3, "high")
                 .when(F.col("max_severity_rank") == 2, "medium")
                 .when(F.col("max_severity_rank") == 1, "low")
                 .otherwise("unclassified")
            )
            .drop("max_severity_rank")
        )

        return controls

**コントロールステータスのロジック:**


    いずれかのリソースがFAILED → コントロール = FAILED
    それ以外ですべてのリソースがPASSED → コントロール = PASSED
    それ以外 → コントロール = UNKNOWN


**例:**


    コントロール S3.1 "S3 Block Public Access"
    ├─ Finding 1: Account 123, Region us-east-1, Bucket A → PASSED
    ├─ Finding 2: Account 123, Region us-east-1, Bucket B → FAILED
    └─ Finding 3: Account 123, Region us-east-1, Bucket C → PASSED

    結果: コントロール S3.1 = FAILED (少なくとも1つが失敗したため)


* * *

### ステップ6: リージョン別サマリー (出力テーブル1)

**このコードが行うこと:**
コントロールレベルのデータをネストしたJSON構造を持つリージョナルコンプライアンスサマリーに集約します。

コントロールをアカウントとリージョンでグループ化し、全体的な合格スコアを計算し、コンプライアンス標準とSeverityレベルによる詳細な内訳を作成します。

**目的:** AWSリージョンごとのコンプライアンスサマリー

**コード参照:**

![](images/icons/grey_arrow_down.png) def aggregate_account_region_summary


    def aggregate_account_region_summary(controls_df, company_id, cf_processed_time):
        """
        Aggregate control-level data to account/region summary.
        Includes per-standard and per-severity breakdowns.
        """
        std_key = ["account_id", "region_id", "standard_id"]

        # Severity-level aggregation
        severity_agg = (
            controls_df
            .groupBy(*std_key, "severity")
            .agg(
                F.countDistinct("control_id").alias("total"),
                F.sum(F.when(F.col("control_status") == "PASSED", 1).otherwise(0)).cast("int").alias("passed")
            )
            .withColumn(
                "score",
                F.round(
                    F.when(F.col("total") > 0, F.col("passed") * 100.0 / F.col("total"))
                     .otherwise(0.0),
                    2
                )
            )
        )

        # Standard-level aggregation
        standards = (
            severity_agg
            .groupBy(*std_key)
            .agg(
                F.sum("total").alias("total"),
                F.sum("passed").alias("passed"),
                F.collect_list(
                    F.struct(
                        F.col("severity").alias("level"),
                        "score",
                        F.struct("total", "passed").alias("controls")
                    )
                ).alias("controls_by_severity")
            )
            .withColumn(
                "score",
                F.round(
                    F.when(F.col("total") > 0, F.col("passed") * 100.0 / F.col("total"))
                     .otherwise(0.0),
                    2
                )
            )
            .select(
                *std_key,
                F.struct(
                    F.col("standard_id").alias("std"),
                    "score",
                    F.struct("total", "passed").alias("controls"),
                    "controls_by_severity"
                ).alias("standard_summary")
            )
        )

        # Account/region summary
        region_key = ["account_id", "region_id"]

        overall = (
            controls_df
            .groupBy(*region_key)
            .agg(
                F.countDistinct(F.struct("standard_id", "control_id")).alias("total_rules"),
                F.sum(F.when(F.col("control_status") == "PASSED", 1).otherwise(0)).cast("int").alias("total_passed")
            )
            .withColumn(
                "control_pass_score",
                F.round(
                    F.when(F.col("total_rules") > 0, (F.col("total_passed") / F.col("total_rules")) * 100)
                     .otherwise(0.0),
                    2
                )
            )
        )

        # Join standards summary
        standards_summary_df = (
            overall
            .join(
                standards.groupBy(*region_key)
                         .agg(F.collect_list("standard_summary").alias("standards_summary")),
                region_key
            )
            .withColumn("cf_processed_time", F.to_timestamp(F.lit(cf_processed_time)))
            .withColumn("company_id", F.lit(company_id))
            .select(
                "company_id",
                "cf_processed_time",
                "account_id",
                "region_id",
                "control_pass_score",
                "total_rules",
                "total_passed",
                "standards_summary"
            )
        )

        return standards_summary_df


**出力スキーマ:**


    {
      "company_id": "xs22xw4aw73q",
      "cf_processed_time": "2024-12-24T00:00:00.000Z",
      "account_id": "123456789012",
      "region_id": "us-east-1",
      "control_pass_score": 78.50,
      "total_rules": 200,
      "total_passed": 157,
      "standards_summary": [
        {
          "std": "aws-foundational-security-best-practices/v/1.0.0",
          "score": 78.50,
          "controls": {"total": 200, "passed": 157},
          "controls_by_severity": [
            {"level": "critical", "score": 85.00, "controls": {"total": 20, "passed": 17}},
            {"level": "high", "score": 75.00, "controls": {"total": 80, "passed": 60}},
            {"level": "medium", "score": 80.00, "controls": {"total": 70, "passed": 56}},
            {"level": "low", "score": 80.00, "controls": {"total": 30, "passed": 24}}
          ]
        }
      ]
    }


* * *

### ステップ7: アカウント別サマリー (出力テーブル2)

**このコードが行うこと:**
リージョナルサマリーデータをアカウントレベルに集約し、各AWSアカウントのすべてのリージョンですべてのルールと合格したコントロールを合計します。

**目的:** すべてのリージョンにわたるアカウントレベルのコンプライアンス

**コード参照:**


    # リージョナルデータをアカウントレベルに集約
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
                    .select(
                        "company_id",
                        "cf_processed_time",
                        "account_id",
                        "score",
                        "total_rules",
                        "total_passed"
                    )
                )

**計算例:**


    アカウント 123456789012:

    リージョン us-east-1:  200ルール, 160合格
    リージョン us-west-2:  200ルール, 150合格
    リージョン eu-west-1:  200ルール, 170合格
    リージョン ap-northeast-1: 200ルール, 160合格
    ─────────────────────────────────────────
    合計:             800ルール, 640合格

    スコア = (640 / 800) × 100 = 80.0000000%


**出力スキーマ:**


    {
      "company_id": "xs22xw4aw73q",
      "cf_processed_at": "2024-12-24T00:00:00.000Z",
      "account_id": "123456789012",
      "score": 80.0000000,
      "total_rules": 800,
      "total_passed": 640
    }


* * *

## S3 エクスポート戦略

### ストレージ構造

**パーティショニング戦略: テーブル → 企業 → 日付**

**なぜこの順序?**

  1. **テーブル中心のクエリ** - 最も一般的なユースケースは「すべてのリージョナルサマリーを取得」または「すべてのアカウントサマリーを取得」

**ストレージの場所:**

    s3://dev-cf-databricks-catalog-bucket/dev/dashboard/compliance/
      ├── standards_summary/
      │   └── company_id=xs22xw4aw73q/
      │       ├── date=2024-12-24/
      │       │   ├── part-00000.csv.gz
      │       │   └── _SUCCESS
      │       ├── date=2024-12-25/
      │       │   ├── part-00000.csv.gz
      │       │   └── _SUCCESS
      │       └── date=2024-12-26/
      │           ├── part-00000.csv.gz
      │           └── _SUCCESS
      │
      └── account_compliance_summary/
          └── company_id=xs22xw4aw73q/
              ├── date=2024-12-24/
              │   ├── part-00000.csv.gz
              │   └── _SUCCESS
              ├── date=2024-12-25/
              │   ├── part-00000.csv.gz
              │   └── _SUCCESS
              └── date=2024-12-26/
                  ├── part-00000.csv.gz
                  └── _SUCCESS

**ファイル説明:**
- `part-00000.csv.gz`: 実際のデータファイル（gzip圧縮CSV）
- `_SUCCESS`: Sparkが自動作成する成功マーカー（0バイト）
  - すべての書き込みタスクが正常完了したことを示す
  - ダウンストリームジョブの開始前チェックに使用可能
  - デバッグに有用（存在しない = 書き込み失敗/未完了）
                  └── part-00000.csv.gz

### 並列書き込み最適化

**Sparkパーティショニング戦略: `.repartition("company_id")`**

**このコードが行うこと:**
企業IDごとにSparkパーティションを作成し、複数企業のデータを並列でS3に書き込みます。各企業のデータは同じSparkパーティションに保持され、企業/日付ごとに~1ファイルが生成されます。

**なぜ `.repartition("company_id")` がベストなのか?**

1. **データ特性に最適** - データは企業ごとに自然に分離されており、企業間の依存関係なし
2. **最大並列度** - 10企業 = 10並列タスク、100企業 = 100並列タスク (線形スケール)
3. **ファイル構造最適化** - 各企業/日付に~1ファイル = Aurora MySQLの取り込みが効率的
4. **書き込み競合なし** - 各企業は別々のS3パーティションに書き込み、ロック競合ゼロ
5. **スケーラビリティ** - 企業数増加時にServerlessが自動スケール、追加設定不要

**Databricks Serverless環境での利点:**

- **自動リソース配分** - 並列タスク数に応じてServerlessが compute リソースを動的に割り当て
- **高速完了** - 最大並列度により実行時間を最小化 = 課金時間を削減
- **弾力性** - 2企業でも100企業でも、最適なリソースを自動的に確保
    (standards_summary_export
     .repartition("company_id")  # 企業ごとに並列書き込み
     .write
     .mode("overwrite")
     .option("header", "true")
     .option("compression", "gzip")  # ストレージ効率のため圧縮
     .option("maxRecordsPerFile", 200000)  # 大企業の場合はファイル分割
     .partitionBy("company_id", "date")  # S3フォルダ構造: company_id=xxx/date=YYYY-MM-DD/
     .csv(standards_s3_path))

    print(f"[WRITE] Standards summary exported to S3: {standards_s3_path}")

    # アカウントサマリーをS3に書き込み (同じ戦略)
    account_summary_export = account_summary.withColumn(
        "date",
        F.date_format("cf_processed_time", "yyyy-MM-dd")
    )

    account_s3_path = f"{s3_base_path}/account_compliance_summary"

    (account_summary_export
     .repartition("company_id")  # 企業ごとに並列書き込み
     .write
     .mode("overwrite")
     .option("header", "true")
     .option("compression", "gzip")
     .option("maxRecordsPerFile", 200000)
     .partitionBy("company_id", "date")
     .csv(account_s3_path))

    print(f"[WRITE] Account compliance summary exported to S3: {account_s3_path}")

**重要な設定:**

| オプション | 説明 | 値 |
|----------|------|-----|
| `.repartition("company_id")` | 企業ごとにSparkパーティション作成 | 10社 = 10並列タスク |
| `.partitionBy("company_id", "date")` | S3フォルダ構造を定義 | `company_id=xxx/date=YYYY-MM-DD/` |
| `.option("compression", "gzip")` | ストレージコスト削減 | CSVファイルを60-70%圧縮 |
| `.option("maxRecordsPerFile", 200000)` | 大企業用のファイル分割 | 1ファイル > 200k行の場合分割 |
| `.mode("overwrite")` | 既存データを上書き | 同じcompany_id/dateを置換 |

### ファイル形式: CSV (gzip圧縮)

**CSVを使用する理由:**

  1. **Aurora MySQLとの互換性** - `LOAD DATA FROM S3`コマンドが直接サポート
  2. **シンプルなパイプライン** - ダウンストリームシステム用のフォーマット変換が不要
  3. **ネストデータ処理** - `standards_summary`カラムはJSON文字列として保存 (Aurora MySQL JSONデータ型に変換可能)
  4. **gzip圧縮** - ストレージコストを60-70%削減、ネットワーク転送も高速化

**トレードオフ:**

  * CSV (gzip) ファイルは未圧縮Parquetより大きいが、Aurora MySQLへの取り込みがシンプル
  * S3でのクエリには遅い (ただしAurora MySQLが主要なクエリエンジン)

**ファイルサイズの例:**

| データ | 未圧縮CSV | CSV (gzip) | 削減率 |
|--------|----------|-----------|--------|
| 500行/企業/日 | 150 KB | 45-60 KB | 60-70% |
| 5,000行/企業/日 | 1.5 MB | 450-600 KB | 60-70% |

### データ保持

**ポリシー: 1年間保持**

**実装方法: S3ライフサイクルポリシー**

    # S3バケットのライフサイクルルール設定例
    {
      "Rules": [
        {
          "Id": "delete-old-compliance-data",
          "Status": "Enabled",
          "Prefix": "dev/dashboard/compliance/",
          "Expiration": {
            "Days": 365
          }
        }
      ]
    }

**理由:**
- コンプライアンス履歴トレンド分析に1年分が必要
- 365日以降のデータは自動削除されてストレージコスト削減



