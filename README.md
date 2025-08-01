# csvRead_arProcessor

## **テーブル名: accounts_receivable(売掛金)**

AIReadで出力したcsvファイルをPostgreSQLに取り込めるように加工するテストデータ作成アプリケーションです。

`save_merged_csvs_to_db_arProcessor.py`を実行すると、データベースに保存されます。


### 実行コマンド

1. 検索条件: `B*030.csv`に絞り込み

    ```
    python copy_filtered_csv_030.py
    ```

2. csvファイルを加工

    ```
    python process_data_arProcessor.py
    ```

3. ヘッダを削除し、ファイルごとにマージ

    ```
    merge_processed_csv_arProcessor.py
    ```

4. マージしたファイルをデータベースに保存

    ```
    python save_merged_csvs_to_db_arProcessor.py
    ```


### ディレクトリ

1. `B*030.csv`に絞り込み

    `G:\共有ドライブ\商工中金\202412_勘定科目明細本番稼働\50_検証\010_反対勘定性能評価\20_テストデータ\作成ワーク\20_売掛金\Import`

2. csvファイルを加工

    `C:\Users\User26\yoko\dev\csvRead_arProcessor\processed_output`

3. ヘッダを削除し、ファイルごとにマージ

`C:\Users\User26\yoko\dev\csvRead_arProcessor\merged_output`

### pgAdminデータ確認方法

```
Servers > PostgreSQL 16(サーバ名) > Databases > nagashin(DB名) > Schemas > public > Tables
```

notes_receivableを右クリック -> View/Edit Data -> All Rows


## レイヤー構成

```
├── master_data/
│   ├── master.csv
│   └── jgroupid_master.csv
├── processed_output/
├── merged_output/
└── scripts/
    ├── copy_filtered_csvs_arProcessor.py
    ├── process_data_arProcessor.py
    ├── merge_processed_csvs_arProcessor.py
    ├── save_merged_csvs_to_db_arProcessor.py
    └── imported_files.log
```

## ルール

### カラム

- **ocr_result_id**: ファイルごとに一意

    最後0で18桁にする `yyyymmddhhmmsssss0`

- **page_no**: 全て`1`で固定

- **id**: ファイルごとに連番

- **jgroupid_string(店番)**: 全て`001`で固定

- **cif_number(顧客番号)**: ファイルごとに一意

    ファイル番号の数字部分6桁(B000050->000050)

- **partner_name_original**,**partner_name(相手先名称)**: ランダム

- **partner_com_code(TKC)**: 頭に`9`を追加した3桁の自動採番でカウントアップ
    支払先に紐づく(partner_nameが同じならpayee_com_codeも同じ)

### データ

- **〃**: 1つ上の項目に合わせる

- **合計**: 行ごと削除(小計、計などの合計行)

## SQL文

```
-- 売掛金(60Columns)

-- 古いテーブルとインデックスがあれば削除
DROP INDEX IF EXISTS idx_jgroupid_string;
DROP TABLE IF EXISTS accounts_receivable;

-- テーブル作成
CREATE TABLE accounts_receivable (
    ocr_result_id CHAR(18) NOT NULL,
    page_no INTEGER NOT NULL,
    id INTEGER NOT NULL,
    jgroupid_string VARCHAR(3),
    cif_number VARCHAR(7),
    settlement_at VARCHAR(6),
    registration_number_original TEXT,
    registration_number TEXT,
    calculation_name_original TEXT,
    calculation_name TEXT,
    partner_name_original TEXT,
    partner_name TEXT,
    partner_location_original TEXT,
    partner_location TEXT,
    partner_location_prefecture TEXT,
    partner_location_city TEXT,
    partner_location_town TEXT,
    partner_location_block TEXT,
    partner_com_code TEXT,
    partner_com_code_status_id INTEGER,
    partner_comcd_relation_source_type_id INTEGER,
    partner_exist_comcd_relation_history_id INTEGER,
    balance_original NUMERIC,
    balance NUMERIC,
    description_original TEXT,
    description TEXT,
    conf_registration_number INTEGER,
    conf_calculation_name INTEGER,
    conf_partner_name INTEGER,
    conf_partner_location INTEGER,
    conf_balance INTEGER,
    conf_description INTEGER,
    coord_x_registration_number NUMERIC,
    coord_y_registration_number NUMERIC,
    coord_h_registration_number NUMERIC,
    coord_w_registration_number NUMERIC,
    coord_x_calculation_name NUMERIC,
    coord_y_calculation_name NUMERIC,
    coord_h_calculation_name NUMERIC,
    coord_w_calculation_name NUMERIC,
    coord_x_partner_name NUMERIC,
    coord_y_partner_name NUMERIC,
    coord_h_partner_name NUMERIC,
    coord_w_partner_name NUMERIC,
    coord_x_partner_location NUMERIC,
    coord_y_partner_location NUMERIC,
    coord_h_partner_location NUMERIC,
    coord_w_partner_location NUMERIC,
    coord_x_balance NUMERIC,
    coord_y_balance NUMERIC,
    coord_h_balance NUMERIC,
    coord_w_balance NUMERIC,
    coord_x_description NUMERIC,
    coord_y_description NUMERIC,
    coord_h_description NUMERIC,
    coord_w_description NUMERIC,
    row_no SMALLINT,
    insertdatetime TIMESTAMP,
    updatedatetime TIMESTAMP,
    updateuser TEXT,
    PRIMARY KEY (ocr_result_id, page_no, id)
);

-- インデックス再作成
CREATE INDEX idx_jgroupid_string ON accounts_receivable(jgroupid_string);
```

```
-- インデックス作成
CREATE INDEX idx_notes_receivable_jgroupid_string ON notes_receivable (jgroupid_string);
CREATE INDEX idx_notes_receivable_cif_number ON notes_receivable (cif_number);
CREATE INDEX idx_notes_receivable_settlement_at ON notes_receivable (settlement_at);
```
