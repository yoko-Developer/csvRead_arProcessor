import os
import psycopg2
import glob

APP_ROOT_DIR = r'C:\Users\User26\yoko\dev\csvRead_arProcessor'
MERGED_OUTPUT_DIR = os.path.join(APP_ROOT_DIR, 'merged_output')
LOG_FILE = os.path.join(APP_ROOT_DIR, 'scripts', 'imported_files.log')

DB_HOST = "localhost"
DB_NAME = "nagashin"
DB_USER = "postgres"
DB_PASSWORD = "x5WU7Xb3"  # 自分のパスワード

def load_imported_files():
    if not os.path.exists(LOG_FILE):
        return set()
    with open(LOG_FILE, 'r', encoding='utf-8') as f:
        return set(line.strip() for line in f if line.strip())

def save_imported_file(filename):
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f"{filename}\n")

def clear_imported_files_log():
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'w', encoding='utf-8') as f:
            f.write('')  # 空にするだけでOK

def save_csvs_to_postgres():
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()

    print("🧹 テーブルとインデックスを初期化中...")
    cur.execute("DROP INDEX IF EXISTS idx_jgroupid_string;")
    cur.execute("DROP TABLE IF EXISTS accounts_receivable;")

    cur.execute("""
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
""")

    cur.execute("CREATE INDEX idx_jgroupid_string ON accounts_receivable(jgroupid_string);")
    conn.commit()
    print("✅ テーブルとインデックスの初期化が完了しました。")

    clear_imported_files_log()
    print("🧹 取り込みログファイルをクリアしました。")

    imported_files = load_imported_files()
    csv_files = glob.glob(os.path.join(MERGED_OUTPUT_DIR, '*.csv'))

    if not csv_files:
        print("📂 マージ済みCSVファイルが見つかりません。")
        cur.close()
        conn.close()
        return

    print(f"📥 {len(csv_files)} 件のファイルを確認中...")

    for csv_file in csv_files:
        filename = os.path.basename(csv_file)
        if filename in imported_files:
            print(f"  ⏭️ スキップ: {filename}（既に取り込み済み）")
            continue

        try:
            print(f"  ⏳ インポート中: {filename}")
            with open(csv_file, 'r', encoding='utf-8-sig') as f:
                cur.copy_expert(
                    sql="COPY accounts_receivable FROM STDIN WITH CSV",
                    file=f
                )
            conn.commit()
            print(f"  ✅ インポート成功: {filename}")
            save_imported_file(filename)
        except Exception as e:
            conn.rollback()
            print(f"  ❌ エラー: {filename} のインポートに失敗しました。エラー内容: {e}")

    cur.close()
    conn.close()
    print("🎉 全CSVのインポート処理が完了しました。")

if __name__ == "__main__":
    save_csvs_to_postgres()
