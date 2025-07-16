import docx
import pandas as pd
import os
import re

# 定数としてテーブルタイプを定義
TABLE_TYPE_HYO = "hyo"
TABLE_TYPE_BETSU_HYO = "betsu_hyo"
TABLE_TYPE_OTHER = "other_tables"
TABLE_TYPE_NO_SPECIFIC_HEADER = "no_specific_header_tables"  # 新しいカテゴリ

def create_dataframe_from_rows(rows, hyo_text, sagyome_text, note_text):
    try:
        df = pd.DataFrame(rows[1:], columns=rows[0])
    except Exception as e:
        print("DataFrame作成エラー:", e)
        return None

    if hyo_text:
        df.insert(0, "表列", hyo_text)
    if sagyome_text:
        df.insert(1, "作業名列", sagyome_text)
    if note_text:
        df["注記"] = note_text

    return df

def extract_tables_from_docx(docx_path):
    document = docx.Document(docx_path)
    extracted_dfs = []

    elements = []
    for block in document.element.body:
        if block.tag.endswith('p'):
            elements.append({'type': 'paragraph', 'content': docx.text.paragraph.Paragraph(block, document)})
        elif block.tag.endswith('tbl'):
            elements.append({'type': 'table', 'content': docx.table.Table(block, document)})

    for i, element in enumerate(elements):
        if element['type'] == 'table':
            table = element['content']

            prev_paragraph_1_text = ""
            prev_paragraph_2_text = ""

            if i > 0 and elements[i - 1]['type'] == 'paragraph':
                prev_paragraph_1_text = elements[i - 1]['content'].text.strip()
            if i > 1 and elements[i - 2]['type'] == 'paragraph':
                prev_paragraph_2_text = elements[i - 2]['content'].text.strip()

            initial_table_classification = TABLE_TYPE_OTHER
            initial_sagyome_col_text = ""
            initial_hyo_col_text = ""

            if re.match(r'^(表|別表)', prev_paragraph_1_text):
                initial_table_classification = TABLE_TYPE_HYO if "表" in prev_paragraph_1_text else TABLE_TYPE_BETSU_HYO
                initial_hyo_col_text = prev_paragraph_1_text
            elif not re.match(r'^(表|別表)', prev_paragraph_1_text) and re.match(r'^(表|別表)', prev_paragraph_2_text):
                initial_table_classification = TABLE_TYPE_HYO if "表" in prev_paragraph_2_text else TABLE_TYPE_BETSU_HYO
                initial_hyo_col_text = prev_paragraph_2_text
                initial_sagyome_col_text = prev_paragraph_1_text
            else:
                initial_table_classification = TABLE_TYPE_NO_SPECIFIC_HEADER

            current_sub_table_rows = []
            building_sub_table = False
            current_sub_table_work_name_internal = ""
            current_sub_table_note = ""

            found_hyo_marker_row_data = None
            row_after_hyo_row_data = None
            is_split_by_marker = False

            for row_idx, row in enumerate(table.rows):
                row_data = [cell.text.strip() for cell in row.cells]
                combined_row_text = "".join(row_data)
                first_cell_text = row_data[0] if row_data else ""

                starts_with_hyo_betsu_hyo_in_row = bool(re.match(r'^(表|別表)', first_cell_text))

                if "(注)" in combined_row_text:
                    if building_sub_table and current_sub_table_rows:
                        note_text_list = []
                        temp_note_row_idx = row_idx
                        while temp_note_row_idx < len(table.rows):
                            current_note_line_cells = [cell.text.strip() for cell in table.rows[temp_note_row_idx].cells]
                            current_note_line_text = "".join(current_note_line_cells).strip()
                            if not current_note_line_text or re.match(r'^(表|別表)', current_note_line_text):
                                break
                            note_text_list.append(current_note_line_text)
                            temp_note_row_idx += 1

                        cleaned_note_list = [line.replace("(注)", "").strip() for line in note_text_list if line.replace("(注)", "").strip()]
                        current_sub_table_note = "\n".join(cleaned_note_list).strip()

                        hyo_col_text = initial_hyo_col_text
                        sagyome_col_text = initial_sagyome_col_text
                        final_table_classification = initial_table_classification

                        if is_split_by_marker:
                            hyo_col_text = "".join(found_hyo_marker_row_data)
                            sagyome_col_text = current_sub_table_work_name_internal
                            final_table_classification = TABLE_TYPE_HYO if re.match(r'^表', hyo_col_text) else TABLE_TYPE_BETSU_HYO

                        df = create_dataframe_from_rows(current_sub_table_rows, hyo_col_text, sagyome_col_text, current_sub_table_note)
                        if df is not None:
                            extracted_dfs.append({'df': df, 'table_type': final_table_classification})

                    current_sub_table_rows = []
                    building_sub_table = False
                    current_sub_table_work_name_internal = ""
                    current_sub_table_note = ""
                    found_hyo_marker_row_data = None
                    row_after_hyo_row_data = None
                    is_split_by_marker = False
                    break

                if starts_with_hyo_betsu_hyo_in_row:
                    if building_sub_table and current_sub_table_rows:
                        hyo_col_text_prev = initial_hyo_col_text
                        sagyome_col_text_prev = initial_sagyome_col_text
                        final_table_classification_prev = initial_table_classification

                        if is_split_by_marker:
                            hyo_col_text_prev = "".join(found_hyo_marker_row_data)
                            sagyome_col_text_prev = current_sub_table_work_name_internal
                            final_table_classification_prev = TABLE_TYPE_HYO if re.match(r'^表', hyo_col_text_prev) else TABLE_TYPE_BETSU_HYO

                        df = create_dataframe_from_rows(current_sub_table_rows, hyo_col_text_prev, sagyome_col_text_prev, current_sub_table_note)
                        if df is not None:
                            extracted_dfs.append({'df': df, 'table_type': final_table_classification_prev})

                    current_sub_table_rows = []
                    building_sub_table = False
                    current_sub_table_work_name_internal = ""
                    current_sub_table_note = ""
                    found_hyo_marker_row_data = row_data
                    row_after_hyo_row_data = None
                    is_split_by_marker = True
                    continue

                if found_hyo_marker_row_data is not None:
                    if row_after_hyo_row_data is None:
                        row_after_hyo_row_data = row_data
                        continue
                    else:
                        new_table_header = row_data if "単位" in combined_row_text else row_after_hyo_row_data
                        new_table_work_name = "".join(row_after_hyo_row_data) if "単位" in combined_row_text else ""

                        current_sub_table_rows = [new_table_header]
                        current_sub_table_work_name_internal = new_table_work_name
                        current_sub_table_note = ""
                        building_sub_table = True

                        found_hyo_marker_row_data = None
                        row_after_hyo_row_data = None
                        continue

                if not building_sub_table and found_hyo_marker_row_data is None and row_after_hyo_row_data is None:
                    current_sub_table_rows = [row_data]
                    current_sub_table_work_name_internal = initial_sagyome_col_text
                    current_sub_table_note = ""
                    building_sub_table = True
                    is_split_by_marker = False
                    continue

                if building_sub_table:
                    current_sub_table_rows.append(row_data)

            if building_sub_table and current_sub_table_rows:
                hyo_col_text_last = initial_hyo_col_text
                sagyome_col_text_last = initial_sagyome_col_text
                final_table_classification_last = initial_table_classification

                if is_split_by_marker and found_hyo_marker_row_data:
                    hyo_col_text_last = "".join(found_hyo_marker_row_data)
                    sagyome_col_text_last = current_sub_table_work_name_internal
                    final_table_classification_last = TABLE_TYPE_HYO if re.match(r'^表', hyo_col_text_last) else TABLE_TYPE_BETSU_HYO

                df = create_dataframe_from_rows(current_sub_table_rows, hyo_col_text_last, sagyome_col_text_last, current_sub_table_note)
                if df is not None:
                    extracted_dfs.append({'df': df, 'table_type': final_table_classification_last})

    return extracted_dfs


def create_dataframe_from_rows(rows, hyo_col_text, sagyome_col_text, note_text):
    """
    ヘルパー関数: 行のリストからDataFrameを作成します。
    「表列」と「作業名列」と「注」カラムを追加します。
    """
    if not rows:
        return None

    headers = rows[0]
    rows_data = rows[1:]

    # ヘッダーの初期設定
    final_headers = []
    
    # 「表列」と「作業名列」の追加ルールに従ってヘッダーを構築
    if hyo_col_text: # 「表列」を追加する場合
        final_headers.append('表')
        if sagyome_col_text: # 「作業名列」も追加する場合
            final_headers.append('作業名')
    elif sagyome_col_text: # 「表列」はないが「作業名列」を追加する場合 (このパスは今回のロジックでは発生しないはずだが念のため)
        final_headers.append('作業名')

    final_headers.extend(headers) # 元のヘッダーを追加

    # 注釈カラムが既になければ追加
    if '注' not in final_headers and note_text is not None:
        final_headers.append('注')
    
    processed_rows_data = []
    for row in rows_data:
        current_row_processed = []
        if hyo_col_text:
            current_row_processed.append(hyo_col_text)
            if sagyome_col_text:
                current_row_processed.append(sagyome_col_text)
        elif sagyome_col_text: # 「表列」はないが「作業名列」を追加する場合
            current_row_processed.append(sagyome_col_text)

        current_row_processed.extend(row) # 元の行データを追加
        current_row_processed.append(note_text) # 注を追加

        processed_rows_data.append(current_row_processed)
            
    # 全ての行とヘッダーで最大の列数を取得し、列数を揃える
    max_cols = len(final_headers)
    if processed_rows_data:
        max_cols = max(max_cols, max(len(r) for r in processed_rows_data))
    
    # final_headers の長さを max_cols に合わせる
    final_headers = final_headers + [''] * (max_cols - len(final_headers))

    # processed_rows_data の各行の長さを max_cols に合わせる
    adjusted_rows_data = []
    for row_data_entry in processed_rows_data:
        adjusted_rows_data.append(row_data_entry + [''] * (max_cols - len(row_data_entry)))

    df = pd.DataFrame(adjusted_rows_data, columns=final_headers)
    return df

def save_dfs_to_csv(dfs_info, output_dir, page_number):
    """
    DataFrameのリスト（とそれに関連する情報）を個別のCSVファイルとして保存します。
    ファイル名は「ページ番号-表の連番.csv」となります。
    table_typeに応じてサブディレクトリを作成します。
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created base output directory: {output_dir}")

    for i, df_info in enumerate(dfs_info):
        df = df_info['df']
        table_type = df_info['table_type'] 

        specific_output_dir = output_dir
        if table_type == TABLE_TYPE_HYO:
            specific_output_dir = os.path.join(output_dir, "hyo")
        elif table_type == TABLE_TYPE_BETSU_HYO:
            specific_output_dir = os.path.join(output_dir, "betsu_hyo")
        elif table_type == TABLE_TYPE_NO_SPECIFIC_HEADER: # 表列/作業名列を追加しなかった表用
            specific_output_dir = os.path.join(output_dir, "other_no_explicit_headers")
        else: # その他のデフォルト（表列・作業名列がついたother_tablesもここに入る可能性があるが、今回は明確な振り分けを優先）
            specific_output_dir = os.path.join(output_dir, "misc_tables")


        if not os.path.exists(specific_output_dir):
            os.makedirs(specific_output_dir)
            print(f"Created output directory: {specific_output_dir}")

        csv_file_name = f"{page_number}-{i+1}.csv"
        csv_file_path = os.path.join(specific_output_dir, csv_file_name)
        
        df.to_csv(csv_file_path, index=False, header=True, encoding='utf-8')
        print(f"Table from page {page_number}, sub-table {i+1} saved to {csv_file_path} in '{table_type}' folder.")


if __name__ == "__main__":
    input_folder_path = "../data/word"  # DOCXファイルを含むフォルダ
    base_output_folder_path = "../tables_from_docx" # CSV出力のベースディレクトリ

    if not os.path.exists(input_folder_path):
        print(f"エラー: 入力フォルダ '{input_folder_path}' が見つかりません。")
    else:
        docx_files = [f for f in os.listdir(input_folder_path) if f.endswith('.docx')]
        
        docx_files.sort(key=lambda f: int(re.findall(r'_page_(\d+)\.docx', f)[0]) if re.findall(r'_page_(\d+)\.docx', f) else 0)

        if not docx_files:
            print(f"'{input_folder_path}' 内にDOCXファイルが見つかりませんでした。")
        else:
            total_extracted_tables = 0
            for docx_file_name in docx_files:
                docx_path = os.path.join(input_folder_path, docx_file_name)
                
                match = re.search(r'_page_(\d+)\.docx', docx_file_name)
                page_number = match.group(1) if match else "unknown_page"
                
                print(f"\nProcessing '{docx_file_name}' (Page {page_number})...")
                extracted_tables_with_types = extract_tables_from_docx(docx_path) 
                
                if extracted_tables_with_types:
                    save_dfs_to_csv(extracted_tables_with_types, base_output_folder_path, page_number)
                    total_extracted_tables += len(extracted_tables_with_types)
                else:
                    print(f"ページ {page_number} のDOCXファイル内に表が見つかりませんでした。")
            
            if total_extracted_tables > 0:
                print(f"\n合計 {total_extracted_tables} 個の表がCSVファイルとして保存されました。")
            else:
                print("\nすべてのDOCXファイルから表が見つかりませんでした。")