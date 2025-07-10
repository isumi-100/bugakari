import pandas as pd
import os
import re

def extract_tables_from_excel(folder_path):
    """
    指定されたフォルダ内のExcelファイルからテーブルを抽出し、CSVとして保存します。

    Excelファイルは '○○_page_i.xlsx' の命名規則に従っていると仮定します。
    各Excelファイル内の各シートは個別のテーブルとして扱われます。
    保存されるCSVファイル名は 'i-j.csv' となります。
    ここで 'i' はExcelファイル名から抽出されたページ番号、
    'j' はExcelファイル内のテーブル（シート）番号です。

    Args:
        folder_path (str): Excelファイルが格納されているフォルダのパス。
    """
    # フォルダが存在するか確認
    if not os.path.isdir(folder_path):
        print(f"エラー: 指定されたフォルダ '{folder_path}' が見つかりません。")
        return

    # フォルダ内のすべてのファイルとディレクトリをリストアップ
    for filename in os.listdir(folder_path):
        # Excelファイル（.xlsx拡張子）のみを処理
        if filename.endswith(".xlsx"):
            file_path = os.path.join(folder_path, filename)

            # ファイル名からページ番号 'i' を抽出
            # 例: '報告書_page_1.xlsx' から '1' を抽出
            match = re.search(r'_page_(\d+)\.xlsx$', filename)
            if match:
                page_number = match.group(1)
            else:
                print(f"警告: ファイル名 '{filename}' からページ番号を抽出できませんでした。このファイルをスキップします。")
                continue

            print(f"'{filename}' を処理中...")

            try:
                # Excelファイルのすべてのシートを読み込む
                # sheet_name=None とすることで、すべてのシートが辞書として読み込まれる
                # キーはシート名、値はDataFrame
                excel_sheets = pd.read_excel(file_path, sheet_name=None)

                table_index = 1
                for sheet_name, df in excel_sheets.items():
                    # CSVファイル名を作成: 'i-j.csv'
                    csv_filename = f"{page_number}-{table_index}.csv"
                    csv_file_path = os.path.join(folder_path, csv_filename)

                    # DataFrameをCSVとして保存
                    df.to_csv(csv_file_path, index=False, encoding='utf-8')
                    print(f"  シート '{sheet_name}' を '{csv_filename}' として保存しました。")
                    table_index += 1

            except Exception as e:
                print(f"エラー: ファイル '{filename}' の処理中に問題が発生しました: {e}")

folder = '../data/excel'
extract_tables_from_excel(folder)
