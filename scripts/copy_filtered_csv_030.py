import os
import shutil
import re

# --- 設定項目 ---
# 検索元フォルダ
INPUT_BASE_DIR = r'G:\共有ドライブ\VLM-OCR\20_教師データ\30_output_csv'
# コピー先フォルダ (新しい共有ドライブのパス)
SEARCH_RESULT_OUTPUT_BASE_DIR = r'G:\共有ドライブ\商工中金\202412_勘定科目明細本番稼働\50_検証\010_反対勘定性能評価\20_テストデータ\作成ワーク\20_売掛金\Import' # ★★★ ここを修正！ ★★★

# 検索パターン (Bで始まり、030.csvで終わるファイル)
SEARCH_PATTERN = r'^B.*030\.csv$'

def copy_filtered_csv_files():
    """
    指定された検索元フォルダから特定のパターンに合致するCSVファイルを検索し、
    指定された出力フォルダにコピーする関数。
    """
    print(f"--- ファイルコピー処理開始 (B*030.csv) ---")
    print(f"検索元フォルダ: {INPUT_BASE_DIR}")
    print(f"コピー先フォルダ: {SEARCH_RESULT_OUTPUT_BASE_DIR}")
    print(f"検索パターン: {SEARCH_PATTERN}")

    # コピー先フォルダが存在しない場合は作成（共有ドライブでも必要に応じて作成を試みる）
    # ただし、共有ドライブのパスによってはos.makedirsが権限エラーになる可能性もあるので注意
    try:
        os.makedirs(SEARCH_RESULT_OUTPUT_BASE_DIR, exist_ok=True)
    except OSError as e:
        print(f"⚠️ 警告: コピー先フォルダの作成に失敗しました。パスを確認してください。エラー: {e}")
        print(f"コピー先フォルダ: {SEARCH_RESULT_OUTPUT_BASE_DIR}")
        # フォルダが存在しないとコピーもできないため、ここで処理を中断するか、手動作成を促す
        return

    copied_count = 0
    skipped_count = 0
    total_files_checked = 0

    # 検索パターンを正規表現オブジェクトとしてコンパイル
    regex = re.compile(SEARCH_PATTERN, re.IGNORECASE)

    for root, dirs, files in os.walk(INPUT_BASE_DIR):
        for filename in files:
            total_files_checked += 1
            # ファイル名が検索パターンに合致するかチェック
            if regex.match(filename):
                src_filepath = os.path.join(root, filename)
                dest_filepath = os.path.join(SEARCH_RESULT_OUTPUT_BASE_DIR, filename)

                try:
                    # ファイルをコピー
                    shutil.copy2(src_filepath, dest_filepath)
                    copied_count += 1
                    # print(f"  コピーしました: {filename}") # 大量に出力される場合はコメントアウト
                except Exception as e:
                    print(f"❌ エラー: {filename} のコピー中に問題が発生しました。エラー: {e}")
                    print(f"  ソース: {src_filepath}")
                    print(f"  宛先: {dest_filepath}")
                    skipped_count += 1
            # else:
            #     print(f"  スキップしました (パターン不一致): {filename}") # デバッグ用

    print(f"\n--- ファイルコピー処理完了 (B*030.csv) ---")
    print(f"✅ 検索元フォルダ内の合計ファイル数: {total_files_checked}")
    print(f"✅ コピーされたファイル数: {copied_count}")
    print(f"⚠️ コピーをスキップしたファイル数 (エラー): {skipped_count}")

if __name__ == "__main__":
    copy_filtered_csv_files()
    