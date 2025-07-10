import os
from PyPDF2 import PdfReader, PdfWriter

def split_pdf_by_pages(input_pdf_path, output_directory="output_pages"):
    """
    PDFファイルを1ページずつ分割し、指定されたディレクトリに保存します。

    Args:
        input_pdf_path (str): 分割したいPDFファイルのパス。
        output_directory (str): 分割されたPDFを保存するディレクトリ。
                                存在しない場合は作成されます。
    """
    # 出力ディレクトリが存在しない場合は作成
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
        print(f"ディレクトリ '{output_directory}' を作成しました。")

    try:
        # PDFファイルを読み込む
        reader = PdfReader(input_pdf_path)
        num_pages = len(reader.pages)
        print(f"'{input_pdf_path}' の総ページ数: {num_pages}")

        # 各ページを個別のファイルとして保存
        for i in range(num_pages):
            writer = PdfWriter()
            writer.add_page(reader.pages[i])

            # 出力ファイル名を生成 (例: original_filename_page_1.pdf)
            base_name = os.path.basename(input_pdf_path)
            file_name_without_ext = os.path.splitext(base_name)[0]
            output_pdf_path = os.path.join(
                output_directory, f"{file_name_without_ext}_page_{i + 1}.pdf"
            )

            with open(output_pdf_path, "wb") as output_file:
                writer.write(output_file)
            print(f"ページ {i + 1} を '{output_pdf_path}' に保存しました。")

        print("\nPDFの分割が完了しました。")

    except FileNotFoundError:
        print(f"エラー: ファイル '{input_pdf_path}' が見つかりません。")
    except Exception as e:
        print(f"PDFの処理中にエラーが発生しました: {e}")

input_file = "../data/001733152.pdf" 
output_folder = "pdf"
split_pdf_by_pages(input_file, output_folder)