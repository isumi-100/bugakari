import pandas as pd
import os
import re

def natural_sort_key(s):
    return [int(text) if text.isdigit() else text.lower()
            for text in re.split(r'([0-9]+)', s)]

def clean_cell_spaces(df):
    """
    各セルの全角・半角スペースを削除
    """
    return df.applymap(lambda x: re.sub(r'[ 　]+', '', x) if isinstance(x, str) else x)

def process_csv_file(file_path):
    """
    1つのCSVファイルに対して前処理を行う。
    セルのスペース除去を行い、上書き保存する。
    """
    try:
        df = pd.read_csv(file_path, encoding='utf-8', dtype=str)
        df_cleaned = clean_cell_spaces(df)
        df_cleaned.to_csv(file_path, index=False, encoding='utf-8')
        return True, None
    except Exception as e:
        return False, str(e)

def process_all_csvs(root_folder):
    """
    指定フォルダ以下のすべてのCSVファイルに対して前処理を実行する。
    """
    total_files = processed = errors = 0

    print(f"Processing CSV files under: {root_folder}\n")

    for dirpath, _, filenames in os.walk(root_folder):
        csv_files = sorted([f for f in filenames if f.endswith('.csv')], key=natural_sort_key)
        for csv_file in csv_files:
            file_path = os.path.join(dirpath, csv_file)
            rel_path = os.path.relpath(file_path, root_folder)
            total_files += 1
            print(f"--- Processing: {rel_path}")
            success, error = process_csv_file(file_path)
            if success:
                print(f"  > Cleaned and saved.")
                processed += 1
            else:
                print(f"  > ERROR: {error}")
                errors += 1
            print("-" * 40)

    print("\n--- Summary ---")
    print(f"Total CSV files found: {total_files}")
    print(f"Successfully processed: {processed}")
    print(f"Errors: {errors}")

# 実行部分
if __name__ == "__main__":
    input_folder = "../tables_from_docx"
    
    if not os.path.exists(input_folder):
        os.makedirs(input_folder)
        print(f"Created folder: {input_folder}. Please place folders with CSV files here.")
    else:
        process_all_csvs(input_folder)
