import pandas as pd
import os
import re # 自然順ソートのために正規表現モジュールをインポート

def natural_sort_key(s):
    """
    ファイル名を自然順にソートするためのキーを生成します。
    例: 'table_10.csv' が 'table_2.csv' の後に来るようにします。
    """
    return [int(text) if text.isdigit() else text.lower()
            for text in re.split('([0-9]+)', s)]

def process_and_combine_csvs(input_folder, output_combined_csv_path):
    """
    指定されたフォルダ内のCSVファイルを読み込み、条件に基づいて結合し、結果を保存します。
    条件に合致しないCSVファイル名はターミナルに出力します。

    Args:
        input_folder (str): CSVファイルが格納されているフォルダのパス。
        output_combined_csv_path (str): 結合されたCSVファイルの出力パス。
    """

    expected_columns = ['作業名', '名称', '摘要', '単位', '所要量', '備考']
    combined_dfs = []
    unprocessed_files = []

    # 統計情報のためのカウンター
    total_files = 0
    combined_files_count = 0
    newline_reason_count = 0
    column_mismatch_reason_count = 0
    unit_empty_reason_count = 0
    other_error_reason_count = 0

    # 出力ディレクトリが存在しない場合は作成
    output_dir = os.path.dirname(output_combined_csv_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")

    # フォルダ内のCSVファイル名をリストアップし、自然順ソート
    csv_files = [f for f in os.listdir(input_folder) if f.endswith(".csv")]
    csv_files.sort(key=natural_sort_key)

    total_files = len(csv_files)

    for filename in csv_files:
        file_path = os.path.join(input_folder, filename)
        
        try:
            df = pd.read_csv(file_path, encoding='utf-8', dtype=str)

            # --- 新機能: 要素内の改行チェック ---
            has_newline = False
            for col in df.columns:
                if df[col].dtype == 'object':
                    if df[col].astype(str).str.contains(r'[\n\r]', regex=True, na=False).any():
                        has_newline = True
                        break
            
            if has_newline:
                print(f"'{filename}': Contains newline character in its cells. Skipping.")
                unprocessed_files.append(filename)
                newline_reason_count += 1
                continue

            columns_to_check_for_empty = ['作業名', '単位', '所要量']
            
            has_empty_in_required_columns = False
            for col_name in columns_to_check_for_empty:
                if col_name in df.columns:
                    # Check for both NaN and empty strings
                    if df[col_name].isnull().any() or (df[col_name] == '').any():
                        print(f"'{filename}': '{col_name}' column contains empty cells. Skipping.")
                        has_empty_in_required_columns = True
                        break
                else:
                    print(f"'{filename}': Required column '{col_name}' not found. Skipping.")
                    has_empty_in_required_columns = True
                    break

            if has_empty_in_required_columns:
                unprocessed_files.append(filename)
                unit_empty_reason_count += 1
                continue

            current_columns = df.columns.tolist()
            
            if current_columns == expected_columns:
                # --- 新機能: '作業名' 列の処理 ---
                # '作業名' が '表' で始まり、かつ '【' を含む場合
                # `df.loc` を使用してSettingWithCopyWarningを回避し、条件に合う行のみを効率的に更新
                condition = df['作業名'].astype(str).str.startswith('表') & \
                            df['作業名'].astype(str).str.contains('【', na=False)

                # 条件に合致する行の'作業名'列を更新
                # re.search() を各要素に適用し、見つかった場合はそのマッチングした部分を、
                # そうでなければ元の値を保持
                df.loc[condition, '作業名'] = df.loc[condition, '作業名'].astype(str).apply(
                    lambda x: re.search(r'【.*', x).group(0) if re.search(r'【.*', x) else x
                )

                combined_dfs.append(df)
                combined_files_count += 1
                print(f"'{filename}': Direct column match. Adding to combine list.")
            else:
                print(f"'{filename}': Columns do not match expected format. Skipping.")
                unprocessed_files.append(filename)
                column_mismatch_reason_count += 1
            
        except Exception as e:
            print(f"Error reading or processing '{filename}': {e}. Skipping.")
            unprocessed_files.append(filename)
            other_error_reason_count += 1

    if combined_dfs:
        final_combined_df = pd.concat(combined_dfs, ignore_index=True)
        final_combined_df.to_csv(output_combined_csv_path, index=False, encoding='utf-8')
        print(f"\nすべての該当するCSVが結合され、'{output_combined_csv_path}' に保存されました。")
    else:
        print("\n結合対象となるCSVファイルが見つかりませんでした。結合されたファイルはありません。")

    if unprocessed_files:
        print("\n--- 結合されなかったCSVファイル ---")
        unprocessed_files.sort(key=natural_sort_key)
        for uf in unprocessed_files:
            print(uf)
    else:
        print("\nすべてのCSVファイルが正常に処理されました。")

    print("\n--- 処理結果の統計 ---")
    print(f"全ファイル数: {total_files} 個")
    print(f"統合されたファイル数: {combined_files_count} 個")
    print(f"統合されなかったファイル数: {(newline_reason_count + column_mismatch_reason_count + unit_empty_reason_count + other_error_reason_count)} 個")
    print(f"  - 原因: セル内に改行コードが含まれる: {newline_reason_count} 個")
    print(f"  - 原因: カラム名が期待される形式と不一致: {column_mismatch_reason_count} 個")
    print(f"  - 原因: '作業名', '単位', '所要量'列に空セルが存在する: {unit_empty_reason_count} 個")
    print(f"  - 原因: その他の読み込みまたは処理エラー: {other_error_reason_count} 個")

# --- 使用例 ---
input_csv_folder = "../data/tables_from_docx"
output_combined_csv_file = os.path.join(input_csv_folder, "combined_tables.csv")

# 実行
print("--- CSVファイル処理を開始します ---")
process_and_combine_csvs(input_csv_folder, output_combined_csv_file)
print("--- CSVファイル処理が完了しました ---")