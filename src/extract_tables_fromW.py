import docx
import pandas as pd
import os
import re

def extract_tables_from_docx(docx_path):
    """
    DOCXファイルからテーブルを抽出し、それぞれのテーブルをDataFrameとして返します。
    「(注)」を含む行を見つけたら現在の表を終了します。
    「表」を含む行が見つかったら、以下のルールで新しい表の開始を判断します。
    1. その「表」を含む行の次の次の行に「単位」という文字が含まれる場合：
       その「次の次の行」を新しい表の開始（ヘッダー）と判断し、
       「表」を含む行の次の行の文字列を「作業名」とします。
    2. 上記以外の場合（「次の次の行」に「単位」が含まれない場合）：
       「表」を含む行の次の行を新しい表の開始（ヘッダー）と判断し、
       「作業名」は空とします。

    ファイル内の最初の表に対しては、表の1つ前の段落の文字列を「作業名」として追加します。
    「作業名」が存在しない場合は、その列を空として追加します。
    """
    document = docx.Document(docx_path)
    extracted_dfs = []
    
    elements = []
    for block in document.element.body:
        if block.tag.endswith('p'): # Paragraph
            elements.append({'type': 'paragraph', 'content': docx.text.paragraph.Paragraph(block, document)})
        elif block.tag.endswith('tbl'): # Table
            elements.append({'type': 'table', 'content': docx.table.Table(block, document)})

    for i, element in enumerate(elements):
        if element['type'] == 'table':
            table = element['content']
            
            # Base '作業名' for the very first segment of this docx.table.Table element
            current_docx_table_preceding_text = "" 
            if i > 0:
                prev_element = elements[i - 1]
                if prev_element['type'] == 'paragraph':
                    current_docx_table_preceding_text = prev_element['content'].text.strip()
            
            current_sub_table_rows = []
            building_sub_table = False 
            current_sub_table_work_name = "" # Stores the '作業名' for the table currently being built
            
            # Flags and buffers for looking ahead after "表"
            found_hyo_marker_row_data = None # Stores data of the "表" row itself
            row_after_hyo_row_data = None    # Stores data of the row immediately after "表" (Row A)
            
            # This flag tracks if the *current* sub-table segment is a result of a split
            is_split_sub_table = False 

            for row_idx, row in enumerate(table.rows):
                row_data = [cell.text.strip() for cell in row.cells]
                combined_row_text = "".join(row_data)

                # --- Condition 1: End current table segment if "(注)" is found ---
                if "(注)" in combined_row_text:
                    if building_sub_table and current_sub_table_rows:
                        # Finalize the current table using its associated work name
                        df = create_dataframe_from_rows(current_sub_table_rows, current_sub_table_work_name)
                        if df is not None:
                            extracted_dfs.append(df)
                    
                    # Reset all state for new table potential
                    current_sub_table_rows = [] 
                    building_sub_table = False
                    current_sub_table_work_name = "" # Reset work name for the next potential table
                    found_hyo_marker_row_data = None
                    row_after_hyo_row_data = None
                    is_split_sub_table = True # Subsequent tables *after* this note are "split"
                    continue # Skip the note row

                # --- Logic for handling "表" marker and looking two rows ahead ---
                # This block activates if a "表" row was found in a previous iteration
                if found_hyo_marker_row_data is not None:
                    if row_after_hyo_row_data is None:
                        # We just passed "表", so this is "Row A" (immediately after "表")
                        row_after_hyo_row_data = row_data
                        continue # Move to "Row B" in the next iteration
                    else:
                        # We have "表" (found_hyo_marker_row_data), "Row A" (row_after_hyo_row_data),
                        # and now this is "Row B" (current row_data).
                        
                        # Finalize any previously built table segment before starting new one
                        if building_sub_table and current_sub_table_rows:
                            df = create_dataframe_from_rows(current_sub_table_rows, current_sub_table_work_name)
                            if df is not None:
                                extracted_dfs.append(df)
                        
                        # --- Determine header and work_name for the *new* table based on "Row B" ---
                        new_table_header = []
                        new_table_work_name = ""

                        # Rule 1: If "Row B" contains "単位"
                        if "単位" in combined_row_text:
                            new_table_header = row_data # "Row B" is the header
                            new_table_work_name = "".join(row_after_hyo_row_data) # "Row A" is the work name
                        else:
                            # Rule 2: "Row B" does NOT contain "単位"
                            new_table_header = row_after_hyo_row_data # "Row A" is the header
                            new_table_work_name = "" # Work name is empty
                        
                        # --- Start the new sub-table ---
                        current_sub_table_rows = [new_table_header] # Initialize with the determined header
                        current_sub_table_work_name = new_table_work_name # Store its specific work name
                        building_sub_table = True # Now building this new table
                        is_split_sub_table = True # This table is a split one
                        
                        # Reset buffers for "表" sequence
                        found_hyo_marker_row_data = None
                        row_after_hyo_row_data = None
                        
                        continue # This row was processed as a header, move to next
                        
                # --- Condition 3: Check if this row is the "表" marker itself ---
                if "表" in combined_row_text:
                    # If we were building a table *before* this "表" marker, finalize it
                    if building_sub_table and current_sub_table_rows:
                        df = create_dataframe_from_rows(current_sub_table_rows, current_sub_table_work_name)
                        if df is not None:
                            extracted_dfs.append(df)
                        
                    # Prepare for the "表" lookahead sequence
                    current_sub_table_rows = [] # Reset for the table that will start *after* this marker
                    building_sub_table = False # Stop building for now, await new header
                    current_sub_table_work_name = "" # Reset work name for the new table sequence
                    found_hyo_marker_row_data = row_data # Store the "表" row itself
                    row_after_hyo_row_data = None # Clear Row A buffer
                    is_split_sub_table = True # Any subsequent tables are now considered "split"
                    continue # Skip this "表" marker row

                # --- Condition 4: Default behavior - start or add row to current sub-table ---
                # If we are not building a table, and no "表" sequence is active,
                # this is the first row of a new table segment
                if not building_sub_table and found_hyo_marker_row_data is None and row_after_hyo_row_data is None:
                    # This implies it's the very first row of the first sub-table
                    # found within a docx.table.Table element that doesn't follow a marker.
                    current_sub_table_rows = [row_data] # This row is the header
                    current_sub_table_work_name = current_docx_table_preceding_text # Use the paragraph text
                    building_sub_table = True
                    is_split_sub_table = False # Not a split table (initial table segment)
                    continue # This row is processed as header

                # If building_sub_table is True, just append the row
                if building_sub_table:
                    current_sub_table_rows.append(row_data)
            
            # After iterating all rows in a docx.table.Table, if there's any remaining data, save it
            if building_sub_table and current_sub_table_rows:
                # Finalize the last table segment using its associated work name
                df = create_dataframe_from_rows(current_sub_table_rows, current_sub_table_work_name)
                if df is not None:
                    extracted_dfs.append(df)

    return extracted_dfs

def create_dataframe_from_rows(rows, preceding_text):
    """Helper function to create a DataFrame from a list of rows."""
    if not rows:
        return None

    headers = rows[0]
    rows_data = rows[1:]

    final_headers = ['作業名'] + headers 
    
    processed_rows_data = []
    for row in rows_data:
        processed_rows_data.append([preceding_text] + row)
            
    max_cols = max(len(final_headers), max(len(row) for row in processed_rows_data) if processed_rows_data else 0)
    
    final_headers = final_headers + [''] * (max_cols - len(final_headers))

    adjusted_rows_data = []
    for row in processed_rows_data:
        adjusted_rows_data.append(row + [''] * (max_cols - len(row)))

    df = pd.DataFrame(adjusted_rows_data, columns=final_headers)
    return df

def save_dfs_to_csv(dfs, output_dir, page_number):
    """
    DataFrameのリストを個別のCSVファイルとして保存します。
    ファイル名は「ページ番号-表の連番.csv」となります。
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")

    for i, df in enumerate(dfs):
        csv_file_name = f"{page_number}-{i+1}.csv"
        csv_file_path = os.path.join(output_dir, csv_file_name)
        
        df.to_csv(csv_file_path, index=False, header=True, encoding='utf-8')
        print(f"Table from page {page_number}, sub-table {i+1} saved to {csv_file_path}")


if __name__ == "__main__":
    input_folder_path = "../data/word"  # Your folder containing DOCX files
    output_folder_path = "../tables_from_docx" # Output directory for CSVs

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
                extracted_tables = extract_tables_from_docx(docx_path)
                
                if extracted_tables:
                    save_dfs_to_csv(extracted_tables, output_folder_path, page_number)
                    total_extracted_tables += len(extracted_tables)
                else:
                    print(f"ページ {page_number} のDOCXファイル内に表が見つかりませんでした。")
            
            if total_extracted_tables > 0:
                print(f"\n合計 {total_extracted_tables} 個の表がCSVファイルとして保存されました。")
            else:
                print("\nすべてのDOCXファイルから表が見つかりませんでした。")