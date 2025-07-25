# csvRead_arProcessor

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

```
G:\共有ドライブ\商工中金\202412_勘定科目明細本番稼働\50_検証\010_反対勘定性能評価\20_テストデータ\作成ワーク\20_売掛金\Import
```

2. csvファイルを加工

```
C:\Users\User26\yoko\dev\csvRead_arProcessor\processed_output
```

3. ヘッダを削除し、ファイルごとにマージ

```
C:\Users\User26\yoko\dev\csvRead_arProcessor\merged_output
```

### pgAdominデータ確認方法

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
