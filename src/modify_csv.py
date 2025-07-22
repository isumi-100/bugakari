import pandas as pd
import os
import re

def natural_sort_key(s):
    return [int(text) if text.isdigit() else text.lower()
            for text in re.split(r'([0-9]+)', s)]

def remove_newlines_from_cell(text):
    """
    Remove newline characters (CR, LF, CRLF) from a string.
    """
    if isinstance(text, str):
        return text.replace('\n', '').replace('\r', '')
    return text

def clean_cell_spaces(df):
    """
    各セルの全角・半角スペースと改行を削除し、カラム名も同様に処理します。
    """
    # カラム名から全角・半角スペースと改行を削除
    df.columns = [re.sub(r'[ 　]+', '', remove_newlines_from_cell(col)) if isinstance(col, str) else col for col in df.columns]
    
    # 各セルから全角・半角スペースと改行を削除
    return df.applymap(lambda x: re.sub(r'[ 　]+', '', remove_newlines_from_cell(x)) if isinstance(x, str) else x)

def reorganize_columns(df):
    """
    「備」列と「考」列が存在し、「考」列が空だったら「考」列を削除し、
    「備」列を「備考」列に名称変更する。
    """
    # Cleaned column names for comparison
    cols = [re.sub(r'[ 　]+', '', col) for col in df.columns]

    has_bi = '備' in cols
    has_kou = '考' in cols

    if has_bi and has_kou:
        # Find the original column names for '備' and '考'
        original_bi_col = df.columns[cols.index('備')]
        original_kou_col = df.columns[cols.index('考')]
        
        # Check if '考' column is entirely empty (or contains only whitespace after cleaning)
        # Using .dropna(how='all') checks if all values are NaN or empty strings after cleaning
        if df[original_kou_col].apply(lambda x: pd.isna(x) or str(x).strip() == '').all():
            print(f"   > '考' column is empty. Removing '考' column.")
            df = df.drop(columns=[original_kou_col])
            
            # Rename '備' to '備考' after potentially dropping '考'
            if original_bi_col in df.columns: # Ensure '備' is still there if '考' was dropped
                print(f"   > Renaming '備' column to '備考'.")
                df = df.rename(columns={original_bi_col: '備考'})
        elif original_bi_col in df.columns: # If '考' is not empty, still rename '備'
            print(f"   > '考' column is not empty. Renaming '備' column to '備考'.")
            df = df.rename(columns={original_bi_col: '備考'})
    elif has_bi: # Only '備' exists
        original_bi_col = df.columns[cols.index('備')]
        print(f"   > Only '備' column found. Renaming '備' column to '備考'.")
        df = df.rename(columns={original_bi_col: '備考'})
        
    return df

def add_bikou_column_if_needed(df, file_path):
    """
    「備考」列がなく「注」列が存在する場合、「注」列の1つ前に「備考」列を追加する。
    変更があった場合、ファイル名をプリントする。
    """
    # Get cleaned column names for robust checking
    cleaned_cols = [re.sub(r'[ 　]+', '', col) for col in df.columns]

    has_bikou = '備考' in cleaned_cols
    has_chu = '注' in cleaned_cols

    if not has_bikou and has_chu:
        # Find the index of the '注' column
        chu_index = cleaned_cols.index('注')
        
        # Create a list of new columns, inserting '備考' at the correct position
        new_columns_order = list(df.columns)
        new_columns_order.insert(chu_index, '備考') # Inserts at chu_index, shifting existing columns
        
        # Reindex the DataFrame to include the new column in the desired position
        df_reindexed = df.reindex(columns=new_columns_order)
        df_reindexed['備考'] = '' # Ensure the new '備考' column is initialized as empty strings

        print(f"   > Added '備考' column before '注' in: {os.path.basename(file_path)}")
        return df_reindexed
    
    return df

def process_csv_file(file_path):
    """
    1つのCSVファイルに対して前処理を行う。
    セルのスペース除去、改行除去、列の整理、および必要に応じた「備考」列の追加を行い、上書き保存する。
    """
    try:
        df = pd.read_csv(file_path, encoding='utf-8', dtype=str)
        
        # Step 1: Clean cell spaces and newlines
        df_cleaned = clean_cell_spaces(df.copy()) # Use a copy to avoid modifying original df in place before column cleaning

        # Step 2: Reorganize columns (handles 備 and 考 -> 備考)
        df_with_reorganized_cols = reorganize_columns(df_cleaned)

        # Step 3: Add 備考 column if not present and 注 column exists
        df_final = add_bikou_column_if_needed(df_with_reorganized_cols, file_path)

        df_final.to_csv(file_path, index=False, encoding='utf-8')
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
                print(f"   > Cleaned and saved.")
                processed += 1
            else:
                print(f"   > ERROR: {error}")
                errors += 1
            print("-" * 40)

    print("\n--- Summary ---")
    print(f"Total CSV files found: {total_files}")
    print(f"Successfully processed: {processed}")
    print(f"Errors: {errors}")

# 実行部分
if __name__ == "__main__":
    input_folder = "../data/tables_from_docx"
    
    if not os.path.exists(input_folder):
        os.makedirs(input_folder)
        print(f"Created folder: {input_folder}. Please place folders with CSV files here.")
    else:
        process_all_csvs(input_folder)