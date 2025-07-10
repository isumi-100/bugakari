import pandas as pd
import os
import re

def natural_sort_key(s):
    return [int(text) if text.isdigit() else text.lower()
            for text in re.split('([0-9]+)', s)]

def process_csv_files(input_folder):
    """
    指定されたフォルダ内のCSVファイルを順番に読み込み、
    カラム名の修正と特定の列の結合を行い、元のファイルを上書き保存します。

    Args:
        input_folder (str): CSVファイルが格納されているフォルダのパス。
    """

    print(f"Processing CSV files in: {input_folder}\n")

    csv_files = [f for f in os.listdir(input_folder) if f.endswith(".csv")]
    csv_files.sort(key=natural_sort_key) # 自然順ソート

    total_files = len(csv_files)
    processed_count = 0
    skipped_count = 0
    column_space_fixed_count = 0
    remark_merged_count = 0
    summary_merged_count = 0
    error_count = 0

    if not csv_files:
        print("No CSV files found in the specified folder.")
        return

    for filename in csv_files:
        file_path = os.path.join(input_folder, filename)
        print(f"--- Processing '{filename}' ---")
        
        try:
            df = pd.read_csv(file_path, encoding='utf-8', dtype=str)
            original_columns = df.columns.tolist()
            modified_this_file = False

            # 1. カラム名のスペースを削除
            new_columns = [col.replace(' ', '').replace('　', '') for col in original_columns]
            if new_columns != original_columns:
                df.columns = new_columns
                print(f"  > Column spaces removed. New columns: {df.columns.tolist()}")
                column_space_fixed_count += 1
                modified_this_file = True

            # 2. 「備考」列の結合と名称変更
            # カラム名を一旦スペース除去後のものに揃えてチェック
            current_cols_no_space = [col.replace(' ', '').replace('　', '') for col in df.columns.tolist()]
            
            if '備' in current_cols_no_space and '考' in current_cols_no_space:
                # 元のDataFrameのカラム名で'備'と'考'のインデックスを取得
                idx_備 = current_cols_no_space.index('備')
                idx_考 = current_cols_no_space.index('考')
                original_備_col_name = original_columns[original_columns.index(df.columns[idx_備])]
                original_考_col_name = original_columns[original_columns.index(df.columns[idx_考])]

                if df[original_考_col_name].isnull().all() or (df[original_考_col_name] == '').all():
                    df = df.drop(columns=[original_考_col_name])
                    df = df.rename(columns={original_備_col_name: '備考'})
                    print(f"  > Merged '{original_備_col_name}' and '{original_考_col_name}' into '備考'.")
                    remark_merged_count += 1
                    modified_this_file = True

            # 3. 「摘要」列の結合と名称変更
            # 再度カラム名をスペース除去後のものに揃えてチェック
            current_cols_no_space = [col.replace(' ', '').replace('　', '') for col in df.columns.tolist()]
            if '摘' in current_cols_no_space and '要' in current_cols_no_space:
                # 元のDataFrameのカラム名で'摘'と'要'のインデックスを取得
                idx_摘 = current_cols_no_space.index('摘')
                idx_要 = current_cols_no_space.index('要')
                original_摘_col_name = original_columns[original_columns.index(df.columns[idx_摘])]
                original_要_col_name = original_columns[original_columns.index(df.columns[idx_要])]

                if df[original_摘_col_name].isnull().all() or (df[original_摘_col_name] == '').all():
                    df = df.drop(columns=[original_摘_col_name])
                    df = df.rename(columns={original_要_col_name: '摘要'})
                    print(f"  > Merged '{original_摘_col_name}' and '{original_要_col_name}' into '摘要'.")
                    summary_merged_count += 1
                    modified_this_file = True
            
            # 変更があった場合のみ上書き保存
            if modified_this_file:
                df.to_csv(file_path, index=False, encoding='utf-8')
                print(f"  > '{filename}' has been **overwritten** with changes.")
                processed_count += 1
            else:
                print(f"  > No changes needed for '{filename}'.")
                skipped_count += 1

        except Exception as e:
            print(f"  > ERROR: Could not process '{filename}'. Reason: {e}")
            error_count += 1
            skipped_count += 1
        print("-" * (len(filename) + 12)) # 区切り線

    print("\n--- Processing Summary ---")
    print(f"Total files found: {total_files}")
    print(f"Files processed (overwritten): {processed_count}")
    print(f"Files skipped (no changes or errors): {skipped_count}")
    print(f"  - Column spaces fixed in: {column_space_fixed_count} files")
    print(f"  - '備考' column merged in: {remark_merged_count} files")
    print(f"  - '摘要' column merged in: {summary_merged_count} files")
    print(f"  - Files with errors: {error_count}")

# --- 実行部分 ---
if __name__ == "__main__":
    input_folder_path = "../data/tables_from_docx" 

    # フォルダが存在しない場合は作成
    if not os.path.exists(input_folder_path):
        os.makedirs(input_folder_path)
        print(f"Created folder: {input_folder_path}. Please place your CSV files here.")
    else:
        process_csv_files(input_folder_path)