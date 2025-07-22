import pandas as pd
import os
import re
from datetime import datetime
import random 
import shutil
import numpy as np 
import json 

# --- 設定項目（ここだけ、くまちゃんの環境に合わせて修正してね！） ---
# 新プロジェクトのルートフォルダ
APP_ROOT_DIR = r'C:\Users\User26\yoko\dev\csvRead_arProcessor' # 売掛金アプリのルート

# B*030.csvをコピーした共有ドライブのパス
INPUT_BASE_DIR = r'G:\共有ドライブ\商工中金\202412_勘定科目明細本番稼働\50_検証\010_反対勘定性能評価\20_テストデータ\作成ワーク\20_売掛金\Import' 

# 加工後のCSVファイルを保存するフォルダ (新プロジェクトのローカルパス)
PROCESSED_OUTPUT_BASE_DIR = os.path.join(APP_ROOT_DIR, 'processed_output') 
# マスタデータファイルが保存されているフォルダ (新プロジェクトのローカルパス)
MASTER_DATA_DIR = os.path.join(APP_ROOT_DIR, 'master_data')
# マージ済みファイルを保存するフォルダ (新プロジェクトのローカルパス)
MERGED_OUTPUT_BASE_DIR = os.path.join(APP_ROOT_DIR, 'merged_output') 


# ★★★ FINAL_POSTGRE_COLUMNS をお客様が提示した売掛金用のリストに完全に一致させる！
FINAL_POSTGRE_COLUMNS = [
    'ocr_result_id',
    'page_no',
    'id',
    'jgroupid_string',
    'cif_number',
    'settlement_at',
    'registration_number_original',   
    'registration_number',       
    'calculation_name_original',    
    'calculation_name',             
    'partner_name_original',        
    'partner_name',                 
    'partner_location_original',    
    'partner_location',             
    'partner_location_prefecture',
    'partner_location_city',
    'partner_location_town',
    'partner_location_block',    
    'partner_com_code',              # maker_com_codeに該当
    'partner_com_code_status_id', 
    'partner_comcd_relation_source_type_id',
    'partner_exist_comcd_relation_history_id',
    'balance_original',             
    'balance',
    'description_original',  
    'description',
    'conf_registration_number', 
    'conf_calculation_name',    
    'conf_partner_name',        
    'conf_partner_location',    
    'conf_balance',             
    'conf_description',         
    'coord_x_registration_number', 
    'coord_y_registration_number', 
    'coord_h_registration_number', 
    'coord_w_registration_number', 
    'coord_x_calculation_name', 
    'coord_y_calculation_name', 
    'coord_h_calculation_name', 
    'coord_w_calculation_name', 
    'coord_x_partner_name',     
    'coord_y_partner_name',     
    'coord_h_partner_name',     
    'coord_w_partner_name',     
    'coord_x_partner_location', 
    'coord_y_partner_location', 
    'coord_h_partner_location', 
    'coord_w_partner_location', 
    'coord_x_balance',          
    'coord_y_balance',          
    'coord_h_balance',          
    'coord_w_balance',          
    'coord_x_description',      
    'coord_y_description',      
    'coord_h_description',      
    'coord_w_description',      
    'insertdatetime',           
    'updatedatetime',           
    'updateuser'                         
]

# 各CSVファイル形式ごとのマッピングルールを定義
ACCOUNTS_RECEIVABLE_MAPPING_DICT = {
    # Postgreカラム名 : 元CSVヘッダー名
    'calculation_name': '科目',
    'partner_name': '相手先名称(氏名)',
    'partner_location': '相手先所在地(住所)',
    'balance': '期末現在高',
    'description': '摘要',
}

# 以前のマッピング辞書も定義は残す (今回は ACCOUNTS_RECEIVABLE_MAPPING_DICT を使う)
HAND_BILL_MAPPING_DICT = {} # 売掛金アプリでは使用しないため空にするか、削除しても良い
FINANCIAL_STATEMENT_MAPPING_DICT = {}
LOAN_DETAILS_MAPPING_DICT = {}
NO_HEADER_MAPPING_DICT = {}


# --- 関数定義 ---
ocr_id_mapping = {}
_ocr_id_sequence_counter = 0 
_ocr_id_fixed_timestamp_str = "" 

def get_ocr_result_id_for_group(file_group_root_name): 
    global ocr_id_mapping
    global _ocr_id_sequence_counter
    global _ocr_id_fixed_timestamp_str

    if file_group_root_name not in ocr_id_mapping:
        sequence_part_int = _ocr_id_sequence_counter * 10
        if sequence_part_int > 99999: 
            sequence_part_int = sequence_part_int % 100000 
        
        sequence_part_str = str(sequence_part_int).zfill(5) 
        
        new_ocr_id = f"{_ocr_id_fixed_timestamp_str}{sequence_part_str}" 

        ocr_id_mapping[file_group_root_name] = new_ocr_id
        _ocr_id_sequence_counter += 1
    
    return ocr_id_mapping[file_group_root_name]

partner_name_to_com_code_map = {} 
next_partner_com_code_val = 100 

def get_partner_com_code_for_name(partner_name): 
    global partner_name_to_com_code_map 
    global next_partner_com_code_val 

    partner_name_str = str(partner_name).strip() 
    
    if not partner_name_str: 
        return "" 

    if partner_name_str in partner_name_to_com_code_map:
        return partner_name_to_com_code_map[partner_name_str]
    else:
        new_code_int = next_partner_com_code_val % 1000 
        if new_code_int < 100: 
            new_code_int = 100 + new_code_int 
        
        new_code_4digit = '3' + str(new_code_int).zfill(3) 
        
        partner_name_to_com_code_map[partner_name_str] = new_code_4digit 
        next_partner_com_code_val += 1
        return new_code_4digit


def process_universal_csv(input_filepath, processed_output_base_dir, input_base_dir, 
                        maker_master_df, ocr_id_map_for_groups, current_file_group_root_name, 
                        final_postgre_columns_list, no_header_map, hand_bill_map, financial_map, loan_map,
                        accounts_receivable_map): 
    """
    全てのAIRead出力CSVファイルを読み込み、統一されたPostgreSQL向けカラム形式に変換して出力する関数。
    CSVの種類（ヘッダー内容）を判別し、それぞれに応じたマッピングを適用する。
    """
    df_original = None
    file_type = "不明" 
    
    try:
        encodings_to_try = ['utf-8', 'utf-8-sig', 'shift_jis', 'cp932']
        
        for enc in encodings_to_try:
            try:
                df_original = pd.read_csv(input_filepath, encoding=enc, header=0, sep=',', quotechar='"', 
                                        dtype=str, na_values=['〃'], keep_default_na=False)
                
                df_original.columns = df_original.columns.str.strip() 
                
                current_headers = df_original.columns.tolist()

                is_accounts_receivable = ('科目' in current_headers) and ('相手先名称(氏名)' in current_headers) and ('期末現在高' in current_headers) and ('摘要' in current_headers)
                
                if is_accounts_receivable: 
                    file_type = "売掛金情報"
                else:
                    file_type = "汎用データ_ヘッダーなし" 
                    df_original = pd.read_csv(input_filepath, encoding=enc, header=None, sep=',', quotechar='"', 
                                            dtype=str, na_values=['〃'], keep_default_na=False)
                    df_original.columns = df_original.columns.astype(str).str.strip() 
                
                print(f"  デバッグ: ファイル {os.path.basename(input_filepath)} の判定結果: '{file_type}'")
                print(f"  デバッグ: 読み込んだ df_original のカラム:\n{df_original.columns.tolist()}")
                print(f"  デバッグ: 読み込んだ df_original の最初の3行:\n{df_original.head(3).to_string()}") 
                print(f"  デバッグ: df_original内の欠損値 (NaN) の数:\n{df_original.isnull().sum().to_string()}") 
                    
                break 
            except Exception as e_inner: 
                print(f"  ファイル {os.path.basename(input_filepath)} を {enc} で読み込み失敗。別のエンコーディングを試します。エラー: {e_inner}")
                df_original = None 
                continue 

        if df_original is None or df_original.empty:
            print(f"  警告: ファイル {os.path.basename(input_filepath)} をどのエンコーディングとヘッダー設定でも読み込めませんでした。処理をスキップします。")
            return 
        
        print(f"  ファイル {os.path.basename(input_filepath)} は '{file_type}' として処理します。")

    except Exception as e:
        print(f"❌ エラー発生（{input_filepath}）: CSV読み込みまたはファイルタイプ判別で問題が発生しました。エラー: {e}")
        import traceback
        traceback.print_exc()
        return

    # --- データ加工処理 ---
    df_data_rows = df_original.copy() 

    if df_data_rows.empty:
        print(f"  警告: ファイル {os.path.basename(input_filepath)} に有効なデータ行が見つからなかったため、加工をスキップします。")
        return 

    # 「〃」マークのみをffillで埋め、空文字列はそのまま維持
    df_data_rows = df_data_rows.ffill() 
    df_data_rows = df_data_rows.fillna('') 
    print(f"  ℹ️ 「〃」マークを直上データで埋め、元々ブランクだった箇所は維持しました。")

    # 合計行の削除ロジック
    keywords_to_delete = ["合計", "小計", "計", "手持手形計", "割引手形計", "その他計"] 
    
    filter_conditions = []
    keywords_regex = r'|'.join([re.escape(k) for k in keywords_to_delete]) 
    
    # 売掛金形式の場合の合計行削除の対象カラム
    if file_type == "売掛金情報":
        if '科目' in df_data_rows.columns: 
            filter_conditions.append(df_data_rows['科目'].str.contains(keywords_regex, regex=True, na=False))
        elif '相手先名称(氏名)' in df_data_rows.columns: 
            filter_conditions.append(df_data_rows['相手先名称(氏名)'].str.contains(keywords_regex, regex=True, na=False))
    elif file_type == "汎用データ_ヘッダーなし":
        if '0' in df_data_rows.columns: 
            filter_conditions.append(df_data_rows['0'].str.contains(keywords_regex, regex=True, na=False))

    if filter_conditions:
        combined_filter = pd.concat(filter_conditions, axis=1).any(axis=1)
        rows_deleted_count = combined_filter.sum()
        df_data_rows = df_data_rows[~combined_filter].reset_index(drop=True)
        if rows_deleted_count > 0:
            print(f"  ℹ️ 合計行（キーワードパターン: {keywords_regex}）を {rows_deleted_count} 行削除しました。")
    
    num_rows_to_process = len(df_data_rows) 
    
    # df_processed の初期化
    df_processed = pd.DataFrame('', index=range(num_rows_to_process), columns=final_postgre_columns_list)


    # --- 共通項目 (PostgreSQLのグリーンの表の左側、自動生成項目) を生成 ---
    df_processed['ocr_result_id'] = [get_ocr_result_id_for_group(current_file_group_root_name)] * num_rows_to_process 

    df_processed['page_no'] = [1] * num_rows_to_process 

    df_processed['id'] = range(1, num_rows_to_process + 1)

    df_processed['jgroupid_string'] = ['001'] * num_rows_to_process

    cif_number_val = current_file_group_root_name[1:] 
    df_processed['cif_number'] = [cif_number_val] * num_rows_to_process

    settlement_at_val = datetime.now().strftime('%Y%m') 
    df_processed['settlement_at'] = [settlement_at_val] * num_rows_to_process

    # --- 各ファイルタイプに応じたマッピングルールを適用 ---
    mapping_to_use = {}
    if file_type == "売掛金情報": 
        mapping_to_use = ACCOUNTS_RECEIVABLE_MAPPING_DICT
    else: 
        mapping_to_use = NO_HEADER_MAPPING_DICT 

    df_data_rows.columns = df_data_rows.columns.astype(str) # 念のためstrに変換
    
    for pg_col_name, src_ref in mapping_to_use.items():
        source_data_series = None
        if isinstance(src_ref, str): 
            if src_ref in df_data_rows.columns: 
                source_data_series = df_data_rows[src_ref]
            else:
                print(f"  ⚠️ 警告: マッピング元のカラム '{src_ref}' が元のCSVファイルに見つかりませんでした（PostgreSQLカラム: {pg_col_name}）。このカラムはブランクになります。")
        elif isinstance(src_ref, int): 
            if str(src_ref) in df_data_rows.columns: 
                source_data_series = df_data_rows[str(src_ref)]
            elif src_ref < df_data_rows.shape[1]: 
                source_data_series = df_data_rows.iloc[:, src_ref]
            else:
                print(f"  ⚠️ 警告: マッピング元の列インデックス '{src_ref}' が元のCSVファイルに存在しません（PostgreSQLカラム: {pg_col_name}）。このカラムはブランクになります。")

        if source_data_series is not None:
            df_processed[pg_col_name] = source_data_series.astype(str).values 
        else:
            pass 


    # --- Excel関数相当のロジックを適用（派生カラムの生成） ---
    df_processed['registration_number_original'] = df_processed['registration_number'].fillna('').astype(str) # registration_number_original を追加	
    df_processed['registration_number'] = df_processed['registration_number'].fillna('').astype(str)
    df_processed['registration_number'] = df_processed['registration_number'].fillna('').astype(str)

    # calculation_name_original, calculation_name
    # calculation_name は ACCOUNTS_RECEIVABLE_MAPPING_DICT で '科目' からマッピング済み
    df_processed['calculation_name_original'] = df_processed['calculation_name'].copy()

    # partner_name_original, partner_name
    # partner_name は ACCOUNTS_RECEIVABLE_MAPPING_DICT で '相手先名称(氏名)' からマッピング済み
    df_processed['partner_name_original'] = df_processed['partner_name'].copy()

    # partner_location_original, partner_location
    # partner_location は ACCOUNTS_RECEIVABLE_MAPPING_DICT で '相手先所在地(住所)' からマッピング済み
    df_processed['partner_location_original'] = df_processed['partner_location'].copy()

    # partner_com_code の処理 (maker_com_code に該当)
    df_processed['partner_com_code'] = df_processed['partner_name'].apply(get_partner_com_code_for_name)

    # balance_original, balance
    # balance は ACCOUNTS_RECEIVABLE_MAPPING_DICT で '期末現在高' からマッピング済み
    def clean_balance_no_comma(value): 
        try:
            cleaned_value = str(value).replace(',', '').strip()
            if not cleaned_value:
                return '' 
            numeric_value = float(cleaned_value)
            return str(int(numeric_value)) 
        except ValueError:
            return '' 
    
    df_processed['balance'] = df_processed['balance'].apply(clean_balance_no_comma)
    df_processed['balance_original'] = df_processed['balance'].copy() 

    # description は ACCOUNTS_RECEIVABLE_MAPPING_DICT で '摘要' からマッピング済み
    df_processed['description_original'] = df_processed['description'].copy() 
    
    # ★★★ 新規追加カラムの固定値設定 ★★★
    df_processed['conf_registration_number'] = '100'
    df_processed['conf_calculation_name'] = '100'
    df_processed['conf_partner_name'] = '100'
    df_processed['conf_partner_location'] = '100'
    df_processed['conf_balance'] = '100'
    df_processed['conf_description'] = '100'

    df_processed['coord_x_registration_number'] = '3000'
    df_processed['coord_y_registration_number'] = '3000'
    df_processed['coord_h_registration_number'] = '3000'
    df_processed['coord_w_registration_number'] = '3000'
    df_processed['coord_x_calculation_name'] = '3000'
    df_processed['coord_y_calculation_name'] = '3000'
    df_processed['coord_h_calculation_name'] = '3000'
    df_processed['coord_w_calculation_name'] = '3000'
    df_processed['coord_x_partner_name'] = '3000'
    df_processed['coord_y_partner_name'] = '3000'
    df_processed['coord_h_partner_name'] = '3000'
    df_processed['coord_w_partner_name'] = '3000'
    df_processed['coord_x_partner_location'] = '3000'
    df_processed['coord_y_partner_location'] = '3000'
    df_processed['coord_h_partner_location'] = '3000'
    df_processed['coord_w_partner_location'] = '3000'
    df_processed['coord_x_balance'] = '3000'
    df_processed['coord_y_balance'] = '3000'
    df_processed['coord_h_balance'] = '3000'
    df_processed['coord_w_balance'] = '3000'
    df_processed['coord_x_description'] = '3000'
    df_processed['coord_y_description'] = '3000'
    df_processed['coord_h_description'] = '3000'
    df_processed['coord_w_description'] = '3000'

    df_processed['insertdatetime'] = ''
    df_processed['updatedatetime'] = ''
    df_processed['updateuser'] = 'testuser'
    
    # --- 保存処理 ---
    processed_output_filename = os.path.basename(input_filepath).replace('.csv', '_processed.csv')
    processed_output_filepath = os.path.join(processed_output_base_dir, processed_output_filename) 
    
    os.makedirs(processed_output_base_dir, exist_ok=True) 
    df_processed.to_csv(processed_output_filepath, index=False, encoding='utf-8-sig')

    print(f"✅ 加工完了: {input_filepath} -> {processed_output_filepath}")

# --- メイン処理 ---
if __name__ == "__main__":
    print(f"--- 処理開始: {datetime.now()} ({APP_ROOT_DIR}) ---") 
    
    _ocr_id_fixed_timestamp_str = datetime.now().strftime('%Y%m%d%H%M')
    print(f"  ℹ️ OCR ID生成の固定時刻: {_ocr_id_fixed_timestamp_str}")

    os.makedirs(PROCESSED_OUTPUT_BASE_DIR, exist_ok=True) 

    MASTER_DATA_DIR = os.path.join(APP_ROOT_DIR, 'master_data') 
    os.makedirs(MASTER_DATA_DIR, exist_ok=True) 

    maker_master_filepath = os.path.join(MASTER_DATA_DIR, 'master.csv') 
    maker_master_df = pd.DataFrame() 
    if os.path.exists(maker_master_filepath):
        try:
            maker_master_df = pd.read_csv(maker_master_filepath, encoding='utf-8')
            print(f"  ℹ️ {maker_master_filepath} を読み込みました (このデータはpartner_com_code生成に利用されます)。")
        except Exception as e:
            print(f"❌ エラー: {maker_master_filepath} の読み込みに失敗しました。エラー: {e}")
            maker_master_df = pd.DataFrame() 
    else:
        print(f"⚠️ 警告: {maker_master_filepath} が見つかりません (partner_com_code生成に影響する可能性があります)。")
        maker_master_df = pd.DataFrame() 


    jgroupid_master_filepath = os.path.join(MASTER_DATA_DIR, 'jgroupid_master.csv')
    jgroupid_values_from_master = [] 
    if os.path.exists(jgroupid_master_filepath): 
        try:
            df_jgroupid_temp = pd.read_csv(jgroupid_master_filepath, encoding='utf-8', header=None)
            
            if not df_jgroupid_temp.empty and df_jgroupid_temp.shape[1] > 0:
                jgroupid_values_from_master = df_jgroupid_temp.iloc[:, 0].astype(str).tolist()
                if not jgroupid_values_from_master:
                    raise ValueError("jgroupid_master.csv からデータを読み込めましたが、リストが空です。")
            else:
                raise ValueError("jgroupid_master.csv が空またはデータがありません。")
            
        except Exception as e:
            print(f"❌ エラー: jgroupid_master.csv の読み込みに失敗しました。エラー: {e}")
            jgroupid_values_from_master = [] 
    else:
        print(f"⚠️ 警告: {jgroupid_master_filepath} が見つかりません。パスを確認してください。")
        jgroupid_values_from_master = [] 

    INPUT_CSV_FILES_DIR = INPUT_BASE_DIR 

    # ocr_result_id のマッピングを事前に生成するロジック
    print("\n--- ocr_result_id マッピング事前生成開始 ---")
    ocr_id_mapping = {}
    _ocr_id_sequence_counter = 0 
    
    all_target_file_groups_root = set() 
    for root, dirs, files in os.walk(INPUT_CSV_FILES_DIR): 
        for filename in files:
            if filename.lower().endswith('.csv') and not filename.lower().endswith('_processed.csv'):
                match = re.match(r'^(B\d{6})_.*\.jpg_030\.csv$', filename, re.IGNORECASE) 
                if match:
                    all_target_file_groups_root.add(match.group(1)) 
                else:
                    print(f"  ℹ️ ファイル名パターンに合致しないファイル: {filename} はocr_result_id生成対象外です。")
                    
    sorted_file_groups_root = sorted(list(all_target_file_groups_root)) 
    
    for group_root_name in sorted_file_groups_root:
        get_ocr_result_id_for_group(group_root_name) 
    
    print("--- ocr_result_id マッピング事前生成完了 ---")
    print(f"生成された ocr_result_id マッピング (最初の5つ): {list(ocr_id_mapping.items())[:5]}...")

    # 生成した ocr_id_mapping をファイルに保存
    ocr_id_map_filepath = os.path.join(MASTER_DATA_DIR, 'ocr_id_mapping_arProcessor.json') 
    try:
        with open(ocr_id_map_filepath, 'w', encoding='utf-8') as f:
            json.dump(ocr_id_mapping, f, ensure_ascii=False, indent=4)
        print(f"  ✅ ocr_id_mapping を {ocr_id_map_filepath} に保存しました。")
    except Exception as e:
        print(f"❌ エラー: ocr_id_mapping の保存に失敗しました。エラー: {e}")

    # メインのファイル処理ループ
    for root, dirs, files in os.walk(INPUT_CSV_FILES_DIR): 
        for filename in files:
            if filename.lower().endswith('.csv') and not filename.lower().endswith('_processed.csv'): 
                input_filepath = os.path.join(root, filename)
                print(f"\n--- 処理対象ファイル: {input_filepath} ---")

                current_file_group_root_name = None
                match = re.match(r'^(B\d{6})_.*\.jpg_030\.csv$', filename, re.IGNORECASE)
                if match:
                    current_file_group_root_name = match.group(1) 
                
                if current_file_group_root_name is None:
                    print(f"  ⚠️ 警告: ファイル {filename} のファイルグループのルート名を特定できませんでした。このファイルはスキップします。")
                    continue 

                process_universal_csv(input_filepath, PROCESSED_OUTPUT_BASE_DIR, INPUT_CSV_FILES_DIR, 
                                    maker_master_df, ocr_id_mapping, current_file_group_root_name, 
                                    FINAL_POSTGRE_COLUMNS, NO_HEADER_MAPPING_DICT, HAND_BILL_MAPPING_DICT, 
                                    FINANCIAL_STATEMENT_MAPPING_DICT, LOAN_DETAILS_MAPPING_DICT,
                                    ACCOUNTS_RECEIVABLE_MAPPING_DICT) 

    print(f"\n🎉 全てのファイルの加工処理が完了しました！ ({datetime.now()}) 🎉")
    