import pandas as pd
import os
import re

def extract_tables_from_excel(excel_path):
    """
    Excelファイルから論理的なテーブルを抽出し、それぞれのテーブルをDataFrameとして返す。
    1列目が「表」または「別表」から始まる行をテーブルの開始点とする。
    空の行、または「(注)」を含む行をテーブルの終了点とする。
    """
    all_extracted_dfs = []

    xls = pd.ExcelFile(excel_path)
    sheet_names = xls.sheet_names

    for sheet_name in sheet_names:
        # シート全体をヘッダーなしで読み込む
        df_full_sheet = pd.read_excel(excel_path, sheet_name=sheet_name, header=None, engine='openpyxl')
        
        # 全てのセルを文字列として読み込み、None を空文字列に変換
        rows_data_list = df_full_sheet.fillna('').astype(str).values.tolist() 

        current_table_rows = [] # 現在構築中のテーブルのデータ行
        building_table = False # テーブル構築中フラグ

        # 行インデックスを制御するための while ループ
        row_idx = 0
        while row_idx < len(rows_data_list):
            row_list = rows_data_list[row_idx]
            combined_row_text = "".join(row_list).strip()
            first_cell_text = row_list[0].strip() if row_list else ""

            # 行が完全に空かどうかの判定
            is_empty_row = all(cell.strip() == '' for cell in row_list)

            # テーブルの開始条件をチェック
            starts_table_marker = re.match(r'^(表|別表)', first_cell_text)

            # --- テーブル終了条件の判定 ---
            # テーブル構築中で、現在の行が空行または注釈行の場合
            if building_table and (is_empty_row or "(注)" in combined_row_text):
                if current_table_rows: # データが1行以上あればテーブルとして確定
                    df = pd.DataFrame(current_table_rows)
                    # Pandasが自動的に最初の行をヘッダーとして認識するようにする
                    # read_excelでheader=Noneにしているので、DataFrame作成後に手動でヘッダーを設定
                    if not df.empty:
                        df.columns = df.iloc[0]
                        df = df[1:].reset_index(drop=True)
                    all_extracted_dfs.append(df)
                
                # 状態をリセット
                current_table_rows = []
                building_table = False
                
                # 注釈行の場合は、その後の関連する注釈をスキップする
                if "(注)" in combined_row_text:
                    temp_note_row_idx = row_idx
                    while temp_note_row_idx < len(rows_data_list):
                        note_row_cells = rows_data_list[temp_note_row_idx]
                        note_row_text = "".join(note_row_cells).strip()
                        # 空行、または1列目が「表」/「別表」で始まる行で注釈の終わりと判断
                        if all(cell.strip() == '' for cell in note_row_cells) or re.match(r'^(表|別表)', note_row_cells[0].strip()):
                            break
                        temp_note_row_idx += 1
                    row_idx = temp_note_row_idx # 注釈ブロックの終わりまで進める
                    continue # この行は既に処理されたので次のループへ
                
                row_idx += 1 # 空行の場合は次の行へ
                continue # この行は既に処理されたので次のループへ

            # --- テーブル開始条件の判定 ---
            # テーブル構築中でなく、現在の行がテーブル開始マーカーの場合
            if not building_table and starts_table_marker:
                # 新しいテーブルの開始なので、現在のテーブルデータはクリア
                current_table_rows = [] 
                building_table = True
                row_idx += 1 # マーカー行自体はテーブルデータではないのでスキップ
                continue # この行は既に処理されたので次のループへ
            
            # --- 通常のデータ行の処理 ---
            # テーブル構築中で、かつ終了条件や開始条件に合致しない場合、行をデータとして追加
            if building_table:
                current_table_rows.append(row_list)
            
            row_idx += 1 # 次の行へ進む

        # シートの終わりに到達した場合、残っているテーブルデータを確定
        if building_table and current_table_rows:
            df = pd.DataFrame(current_table_rows)
            if not df.empty:
                df.columns = df.iloc[0]
                df = df[1:].reset_index(drop=True)
            all_extracted_dfs.append(df)

    # シンプルなロジックではテーブルタイプを振り分けないため、
    # 全てのDataFrameを 'extracted_tables' タイプとして返す
    return [{'df': df, 'table_type': 'extracted_tables'} for df in all_extracted_dfs if not df.empty]

def save_dfs_to_csv(dfs_info, output_dir, file_name_prefix):
    """
    DataFrameのリスト（とそれに関連する情報）を個別のCSVファイルとして保存します。
    ファイル名は「ファイル名プレフィックス-表の連番.csv」となります。
    特定のサブディレクトリに保存します。
    """
    # 常に 'extracted_tables' サブディレクトリに保存
    specific_output_dir = os.path.join(output_dir, "extracted_tables") 
    
    if not os.path.exists(specific_output_dir):
        os.makedirs(specific_output_dir)
        print(f"Created output directory: {specific_output_dir}")

    for i, df_info in enumerate(dfs_info):
        df = df_info['df']
        
        csv_file_name = f"{file_name_prefix}-{i+1}.csv"
        csv_file_path = os.path.join(specific_output_dir, csv_file_name)
        
        df.to_csv(csv_file_path, index=False, header=True, encoding='utf-8')
        print(f"Table {i+1} from '{file_name_prefix}' saved to {csv_file_path}")


if __name__ == "__main__":
    input_folder_path = "../data/excel"  # Excelファイルを含むフォルダ
    base_output_folder_path = "../data/csv" # CSV出力のベースディレクトリ (新しいフォルダ名)

    if not os.path.exists(input_folder_path):
        print(f"エラー: 入力フォルダ '{input_folder_path}' が見つかりません。")
    else:
        excel_files = [f for f in os.listdir(input_folder_path) if f.endswith('.xlsx')]
        
        # ページ番号に基づいてファイルを数値としてソート
        def sort_key_numeric_page(f_name):
            match = re.findall(r'_page_(\d+)\.xlsx', f_name)
            if match:
                return int(match[0])
            return float('inf') # 数字がないファイルは最後に
            
        excel_files.sort(key=sort_key_numeric_page)

        if not excel_files:
            print(f"'{input_folder_path}' 内にExcelファイルが見つかりませんでした。")
        else:
            total_extracted_tables = 0
            for excel_file_name in excel_files:
                excel_path = os.path.join(input_folder_path, excel_file_name)
                
                file_name_prefix = os.path.splitext(excel_file_name)[0]
                
                print(f"\nProcessing '{excel_file_name}'...")
                extracted_tables_info = extract_tables_from_excel(excel_path) 
                
                if extracted_tables_info:
                    save_dfs_to_csv(extracted_tables_info, base_output_folder_path, file_name_prefix)
                    total_extracted_tables += len(extracted_tables_info)
                else:
                    print(f"'{excel_file_name}' 内に表が見つかりませんでした。")
            
            if total_extracted_tables > 0:
                print(f"\n合計 {total_extracted_tables} 個の表がCSVファイルとして保存されました。")
            else:
                print("\nすべてのExcelファイルから表が見つかりませんでした。")