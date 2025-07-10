import docx
import pandas as pd
import os

def extract_tables_from_docx(docx_path):
    """
    DOCXファイルからテーブルを抽出し、それぞれのテーブルをDataFrameとして返します。
    ただし、表のヘッダーに「摘」と「要」の文字を**両方とも**含むもののみを抽出し、
    その際、表の1つ前の段落の文字列を「作業名」として追加します。
    「作業名」が存在しない場合は、その列を空として追加します。
    """
    document = docx.Document(docx_path)
    extracted_dfs = []
    
    # ドキュメント内のすべてのブロック要素（段落、表）を順番に取得し、リストに格納
    elements = []
    for block in document.element.body:
        if block.tag.endswith('p'): # Paragraph
            elements.append({'type': 'paragraph', 'content': docx.text.paragraph.Paragraph(block, document)})
        elif block.tag.endswith('tbl'): # Table
            elements.append({'type': 'table', 'content': docx.table.Table(block, document)})

    # elementsリストをループし、表を抽出
    for i, element in enumerate(elements):
        if element['type'] == 'table':
            table = element['content']
            
            # 作業名として使用するテキストの初期化
            preceding_text_for_column = "" # デフォルトを空文字列に変更

            # 表の1つ前の要素が存在するか確認し、「作業名」を取得
            if i > 0:
                prev_element = elements[i - 1]
                if prev_element['type'] == 'paragraph':
                    preceding_text_for_column = prev_element['content'].text.strip()
            
            # テーブルのデータを一時的に抽出してヘッダーをチェック
            temp_data = []
            for row_idx, row in enumerate(table.rows):
                row_data = [cell.text.strip() for cell in row.cells]
                temp_data.append(row_data)
                if row_idx == 0: # 最初の行をヘッダーとしてチェック
                    current_headers = row_data
                    break # ヘッダーのみ取得すればよいのでループを抜ける

            is_target_table = False
            # ヘッダーに「摘」と「要」が含まれているかチェック
            # 全てのヘッダーセルを結合してチェックすることで、「摘　　要」のように分かれていても対応
            combined_headers = "".join(current_headers)
            if "摘" in combined_headers and "要" in combined_headers:
                is_target_table = True

            # 抽出条件に合致する場合のみ処理
            if is_target_table:
                data = []
                for row in table.rows:
                    row_data = [cell.text for cell in row.cells]
                    data.append(row_data)
                
                if data: # データがある場合のみDataFrameを生成
                    headers = data[0]
                    rows_data = data[1:]

                    # ヘッダーに「作業名」を追加
                    final_headers = ['作業名'] + headers 
                    
                    # 各データ行の先頭に作業名を追加
                    processed_rows_data = []
                    for row in rows_data:
                        processed_rows_data.append([preceding_text_for_column] + row)
                            
                    # DataFrameを作成する前に、ヘッダーとデータ行の列数を揃える
                    max_cols = max(len(final_headers), max(len(row) for row in processed_rows_data) if processed_rows_data else 0)
                    
                    # ヘッダーの列数を調整
                    final_headers = final_headers + [''] * (max_cols - len(final_headers))

                    # データ行の列数を調整（足りない場合は空文字列で埋める）
                    adjusted_rows_data = []
                    for row in processed_rows_data:
                        adjusted_rows_data.append(row + [''] * (max_cols - len(row)))

                    df = pd.DataFrame(adjusted_rows_data, columns=final_headers)
                    
                    extracted_dfs.append(df)
                    print(f"Extracted table {len(extracted_dfs)} from DOCX (matching criteria).")
            # else: 抽出条件に合わない表はスキップされる

    return extracted_dfs

def save_dfs_to_csv(dfs, output_dir="tmp"):
    """
    DataFrameのリストを個別のCSVファイルとして保存します。
    各DataFrameの最初の行がヘッダーとして扱われ、CSVにもヘッダーとして出力されます。
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")

    for i, df in enumerate(dfs):
        csv_file_path = os.path.join(output_dir, f"table_{i+1}.csv")
        # DataFrameのヘッダーをCSVの1行目として出力
        df.to_csv(csv_file_path, index=False, header=True, encoding='utf-8')
        print(f"Table {i+1} saved to {csv_file_path}")

# DOCXファイルのパスを指定してください
docx_file_path = "../data/word/001733152_page_11.docx"

if __name__ == "__main__":
    if os.path.exists(docx_file_path):
        extracted_tables = extract_tables_from_docx(docx_file_path)
        if extracted_tables:
            save_dfs_to_csv(extracted_tables)
            print("\n抽出条件に合致するすべての表が個別のCSVファイルとして保存されました。")
        else:
            print("抽出条件に合致する表がDOCXファイル内に見つかりませんでした。")
    else:
        print(f"エラー: DOCXファイル '{docx_file_path}' が見つかりません。")