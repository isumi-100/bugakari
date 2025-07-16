import pandas as pd
import os
import re

# 正規表現で表のソートキーを抽出する関数
def extract_table_sort_keys(table_value, prefix):
    """
    '表'列の値からソート用の数値キー（例: '表RA-2-12' -> (2, 12)）を抽出します。
    指定されたprefix（'表'または'別表'）で始まることを期待します。
    マッチしない場合は無限大を返し、末尾に配置します。
    """
    match = re.search(rf'^{prefix}.*?-(\d+)-(\d+)', str(table_value))
    if match:
        return int(match.group(1)), int(match.group(2))
    else:
        return float('inf'), float('inf')  # マッチしないものは末尾へ

# '表'列の値が特定のパターンに合致するかチェックするヘルパー関数
def is_valid_table_id_pattern(df_column, prefix):
    """
    DataFrameの指定された列のすべての値が、指定されたprefixで始まり、
    かつ特定の正規表現パターンに合致するかどうかをチェックします。
    例: '表RA-2-12【設】【専】' のような形式に対応。
    """
    if df_column.empty:
        return False
    # re.fullmatch を使用して文字列全体がパターンに合致するか確認
    # (?:【.*?】)*$ で、末尾に0回以上の【任意文字】のブロックがあることを許容
    return df_column.astype(str).apply(lambda x: re.fullmatch(rf'^{prefix}.*?-\d+-\d+(?:【.*?】)*$', x)).all()

# 自然順ソートのためのキーを生成する関数
def natural_sort_key(s):
    """
    ファイル名などを自然順（数字部分を数値として）でソートするためのキーを生成します。
    """
    return [int(text) if text.isdigit() else text.lower() for text in re.split('([0-9]+)', s)]

# 特殊なGroup1形式のCSVを処理する関数（単一列の変換と作業名へのマージ）
def handle_special_group1_case(df_original, file_path, expected_base_columns):
    """
    特定の列構造を持つDataFrameをGroup1形式に変換します。
    この関数は、'単位'と'備考'の間に単一の、'所要量'ではない列が存在する場合に適用されます。
    その列名を'作業名'にマージし、列を'所要量'にリネームします。

    Args:
        df_original (pd.DataFrame): 処理対象の元のDataFrame。
        file_path (str): 処理中のファイルのパス（ログ出力用）。
        expected_base_columns (list): 期待される基本列名のリスト。

    Returns:
        tuple: (変換されたDataFrame, エラーメッセージ)。
               変換が成功した場合は(pd.DataFrame, None)、失敗した場合は(None, str)を返します。
    """
    df = df_original.copy() # オリジナルCSVは編集しないため、DataFrameのコピーを操作
    col_list = df.columns.tolist()

    # 期待される列のプレフィックスとサフィックスを定義
    expected_prefix_cols = expected_base_columns[0:5] # ['表', '作業名', '名称', '摘要', '単位']
    expected_suffix_col = expected_base_columns[6] # '備考'

    # 1. 最小限の列数と主要な列の存在チェック
    if not ('単位' in col_list and '備考' in col_list and '作業名' in col_list and '表' in col_list):
        return None, "必要な列('表', '作業名', '単位', '備考')が見つかりません。"

    # 2. プレフィックス列が期待されるものと一致するかチェック
    if col_list[0:5] != expected_prefix_cols:
        return None, "プレフィックス列が期待されるものと一致しません。"

    # 3. '備考'列が期待される位置（インデックス6）にあるかチェック
    if len(col_list) <= 6 or col_list[6] != expected_suffix_col:
        return None, "'備考'列が期待される位置にありません。"

    # 4. インデックス5の列が'所要量'ではないかチェック (単一列展開の条件)
    col_to_merge_name = col_list[5]
    if col_to_merge_name == '所要量':
        return None, "インデックス5の列は既に'所要量'です（特殊ケースではありません）。"
    
    # 5. '単位'と'備考'の間に他の列がないかチェック (単一列展開の条件)
    try:
        idx_unit = col_list.index('単位')
        idx_remarks = col_list.index('備考')
        if not (idx_remarks - idx_unit - 1 == 1): # '単位'と'備考'の間に1つだけ列があることを確認
            return None, "'単位'と'備考'の間に単一の列がありません。"
    except ValueError:
        return None, "'単位'または'備考'列のインデックスが見つかりません。"

    # 全ての条件を満たした場合、変換処理を実行
    # print(f"  > 特殊なGroup1形式の変換を適用中 (単一列展開): {file_path}") # メインループで出力

    # 該当する列名を作業列名のセルに文字列マージ
    df['作業名'] = df['作業名'].astype(str) + col_to_merge_name

    # 該当する列名を'所要量'にリネーム
    df = df.rename(columns={col_to_merge_name: '所要量'})

    # 列の順序をexpected_base_columnsに合わせ、残りの列は末尾に配置
    current_cols = df.columns.tolist()
    ordered_cols = []
    
    for col in expected_base_columns:
        if col in current_cols:
            ordered_cols.append(col)
    
    for col in current_cols:
        if col not in expected_base_columns and col not in ordered_cols:
            ordered_cols.append(col)
            
    df = df[ordered_cols]

    return df, None # 変換されたDataFrameとエラーなしを返す

# 複数列を展開して行に変換する特殊なGroup1形式のCSVを処理する関数
def handle_multi_column_expansion_case(df_original, file_path, expected_base_columns):
    """
    '単位'と'備考'の間に複数の所要量相当の列があるDataFrameを、
    それらの列を展開して行に変換し、'作業名'に列名をマージする形式に変換します。

    Args:
        df_original (pd.DataFrame): 処理対象の元のDataFrame。
        file_path (str): 処理中のファイルのパス（ログ出力用）。
        expected_base_columns (list): 期待される基本列名のリスト。

    Returns:
        tuple: (変換されたDataFrame, エラーメッセージ)。
               変換が成功した場合は(pd.DataFrame, None)、失敗した場合は(None, str)を返します。
    """
    df = df_original.copy()
    col_list = df.columns.tolist()

    # 期待される基本列のプレフィックスとサフィックス
    expected_prefix_cols = expected_base_columns[0:5] # ['表', '作業名', '名称', '摘要', '単位']
    expected_suffix_col = expected_base_columns[6] # '備考'

    # 1. '所要量'列が既に存在しないかチェック
    if '所要量' in col_list:
        return None, "DataFrameに既に'所要量'列が存在します。"

    # 2. 必要な主要列の存在と列数のチェック
    if not ('単位' in col_list and '備考' in col_list and '作業名' in col_list and '表' in col_list):
        return None, "必要な列('表', '作業名', '単位', '備考')が見つかりません。"

    try:
        idx_unit = col_list.index('単位')
        idx_remarks = col_list.index('備考')
    except ValueError:
        return None, "'単位'または'備考'列のインデックスが見つかりません。"

    # 3. '単位'が'備考'より前にあり、その間に複数の列があるかチェック (複数列展開の条件)
    if not (idx_unit < idx_remarks - 1 and (idx_remarks - idx_unit - 1) > 1):
        return None, "'単位'と'備考'の間に複数の列がありません。"

    # 4. プレフィックス列が期待されるものと一致するかチェック
    if col_list[0:5] != expected_prefix_cols:
        return None, "プレフィックス列が期待されるものと一致しません。"
        
    # 展開対象となる列（'単位'と'備考'の間の列）を特定
    columns_to_melt = col_list[idx_unit + 1 : idx_remarks]
    
    # 展開対象列が全て数字に変換できるか（空文字列はNaNになり得るので考慮）
    # is_numeric = df[columns_to_melt].apply(lambda s: pd.to_numeric(s, errors='coerce').notna().all()).all()
    # if not is_numeric:
    #     return None, "展開対象列に数値に変換できない値が含まれています。"

    # print(f"  > 特殊なGroup1形式の変換を適用中 (複数列展開): {file_path}") # メインループで出力

    # ID列として残す列（展開しない列）
    id_vars = col_list[0:idx_unit+1] + col_list[idx_remarks:] # '単位'までと'備考'以降の列

    # melt（展開）処理
    df_melted = df.melt(id_vars=id_vars,
                         value_vars=columns_to_melt,
                         var_name='展開列名', # 一時的に展開元の列名を保持する新しい列名
                         value_name='所要量') # 展開後の数値が格納される列名

    # '作業名'に展開元の列名を結合
    df_melted['作業名'] = df_melted['作業名'].astype(str) + df_melted['展開列名'].astype(str)
    
    # 不要になった一時列を削除
    df_melted = df_melted.drop(columns=['展開列名'])

    # 列の順序をexpected_base_columnsに合わせ、残りの列は末尾に配置
    current_cols_melted = df_melted.columns.tolist()
    ordered_cols = []
    
    for col in expected_base_columns:
        if col in current_cols_melted:
            ordered_cols.append(col)
    
    for col in current_cols_melted:
        if col not in expected_base_columns and col not in ordered_cols:
            ordered_cols.append(col)
            
    df_melted = df_melted[ordered_cols]

    return df_melted, None # 変換されたDataFrameとエラーなしを返す


# ネストされたCSVファイルを処理し、グループ分けとマージを行う関数
def process_nested_csvs(root_folder, output_combined_main_csv_path, output_combined_annex_csv_path):
    """
    指定されたルートフォルダ以下の全てのCSVファイルを探索し、
    その列構造と'表'列の内容に基づいてグループに分類します。
    '表'で始まるGroup1ファイルと'別表'で始まるGroup1ファイルはそれぞれ結合され、
    個別のCSVファイルとして出力されます。
    """
    group1_main_dfs = [] # '表'で始まるGroup1のDataFrameリスト
    group1_annex_dfs = [] # '別表'で始まるGroup1のDataFrameリスト
    group2_files = [] # Group2に分類されたファイルパスを格納
    group3_files = [] # その他のファイル + エラーファイル
    group4_files = [] # カラム名はGroup1の構造に合うが、'表'列が'表'でも'別表'でもないファイル

    # 期待される基本列の定義
    expected_base_columns = ['表', '作業名', '名称', '摘要', '単位', '所要量','備考']
    # Group2の最小限の必須列の定義
    min_group2_columns = set(['名称', '摘要', '単位', '所要量','備考'])

    all_csv_paths = []

    # root_folder以下の全てのCSVファイルのパスを収集
    for dirpath, _, filenames in os.walk(root_folder):
        for file in filenames:
            if file.endswith('.csv'):
                all_csv_paths.append(os.path.join(dirpath, file))

    # 収集したCSVファイルを一つずつ処理
    for file_path in all_csv_paths:
        try:
            df = pd.read_csv(file_path, encoding='utf-8', dtype=str)
            # カラム名の前後の空白を削除
            df.columns = [col.strip() for col in df.columns]

            col_list = df.columns.tolist() # 現在のDataFrameの列名リスト

            # '表'列が存在しない、または空のDataFrameの場合はGroup3へ
            if '表' not in col_list or df.empty:
                print(f"Group3 (必須列'表'がないか空のファイル): {file_path}")
                group3_files.append(file_path)
                continue

            # '表'列の最初の値に基づいてプレフィックスを判定
            first_table_id_value = str(df['表'].iloc[0])
            current_table_id_prefix = None
            if first_table_id_value.startswith('表') and not first_table_id_value.startswith('別表'):
                current_table_id_prefix = '表'
            elif first_table_id_value.startswith('別表'):
                current_table_id_prefix = '別表'

            # Group1への分類を試みるフラグ
            is_classified_as_group1 = False
            
            # --- Group 1 の分類ロジック ---
            # 1. 厳密なGroup1の列構造を試す
            if len(col_list) >= 7 and col_list[:7] == expected_base_columns:
                if current_table_id_prefix and is_valid_table_id_pattern(df['表'], current_table_id_prefix):
                    print(f"Group1 ({current_table_id_prefix} - 厳密な一致): {file_path}")
                    target_dfs_list = group1_main_dfs if current_table_id_prefix == '表' else group1_annex_dfs
                    target_dfs_list.append((df, file_path))
                    is_classified_as_group1 = True
            
            # 2. 特殊なGroup1形式 (単一列の変換) を試す
            if not is_classified_as_group1:
                modified_df, error_msg = handle_special_group1_case(df, file_path, expected_base_columns)
                if modified_df is not None: # 構造変換が成功した場合
                    # 変換後のDataFrameの'表'列が正しいパターンに合致するかチェック
                    if current_table_id_prefix and is_valid_table_id_pattern(modified_df['表'], current_table_id_prefix):
                        print(f"Group1 ({current_table_id_prefix} - 単一列変換): {file_path}")
                        target_dfs_list = group1_main_dfs if current_table_id_prefix == '表' else group1_annex_dfs
                        target_dfs_list.append((modified_df, file_path))
                        is_classified_as_group1 = True
            
            # 3. 特殊なGroup1形式 (複数列の展開) を試す
            if not is_classified_as_group1:
                modified_df, error_msg = handle_multi_column_expansion_case(df, file_path, expected_base_columns)
                if modified_df is not None: # 構造変換が成功した場合
                    # 変換後のDataFrameの'表'列が正しいパターンに合致するかチェック
                    if current_table_id_prefix and is_valid_table_id_pattern(modified_df['表'], current_table_id_prefix):
                        print(f"Group1 ({current_table_id_prefix} - 複数列展開): {file_path}")
                        target_dfs_list = group1_main_dfs if current_table_id_prefix == '表' else group1_annex_dfs
                        target_dfs_list.append((modified_df, file_path))
                        is_classified_as_group1 = True
            
            # Group1として分類された場合は次のファイルへ
            if is_classified_as_group1:
                continue 

            # Group1として分類されなかった場合の処理
            # Group4: '表'列が'表'でも'別表'でも始まらない場合
            if current_table_id_prefix is None:
                print(f"Group4 (表/別表判定不可): {file_path}")
                group4_files.append(file_path)
                continue

            # --- Group 2 条件 ---
            # Group1/Group4として分類されなかった場合のみGroup2をチェック
            present_cols = set(col_list)
            if len(min_group2_columns - present_cols) == 1:
                print(f"Group2: {file_path}")
                group2_files.append(file_path)
                continue

            # --- Group 3 に分類 ---
            # 上記のどのグループにも属さない場合
            print(f"Group3 (その他): {file_path}")
            group3_files.append(file_path)

        except Exception as e:
            print(f"ファイル '{file_path}' の処理中にエラーが発生しました: {e}")
            group3_files.append(file_path)

    # Group1 (表)をソートしてマージ
    if group1_main_dfs:
        group1_main_dfs.sort(key=lambda x: extract_table_sort_keys(x[0].iloc[0]['表'], prefix='表'))
        combined_main_df = pd.concat([item[0] for item in group1_main_dfs], ignore_index=True)
        combined_main_df.to_csv(output_combined_main_csv_path, index=False, encoding='utf-8')
        print(f"\nGroup1 (表) マージ済みCSVを出力しました: {output_combined_main_csv_path}")
    else:
        print("\nGroup1 (表) に該当するCSVがありませんでした。")

    # Group1 (別表)をソートしてマージ
    if group1_annex_dfs:
        group1_annex_dfs.sort(key=lambda x: extract_table_sort_keys(x[0].iloc[0]['表'], prefix='別表'))
        combined_annex_df = pd.concat([item[0] for item in group1_annex_dfs], ignore_index=True)
        combined_annex_df.to_csv(output_combined_annex_csv_path, index=False, encoding='utf-8')
        print(f"\nGroup1 (別表) マージ済みCSVを出力しました: {output_combined_annex_csv_path}")
    else:
        print("\nGroup1 (別表) に該当するCSVがありませんでした。")

    # 結果表示
    print("\n--- グループ分け結果 ---")
    print(f"Group1 (表): {len(group1_main_dfs)} ファイル")
    print(f"Group1 (別表): {len(group1_annex_dfs)} ファイル")
    print(f"Group2: {len(group2_files)} ファイル")
    for f in sorted(group2_files, key=natural_sort_key):
        print(f" - {f}")
    print(f"Group4 (カラム名一致だが表/別表判定不可): {len(group4_files)} ファイル")
    for f in sorted(group4_files, key=natural_sort_key):
        print(f" - {f}")
    print(f"Group3 (その他/エラー): {len(group3_files)} ファイル")
    # Group3のファイル名も表示したい場合は以下のコメントを外す
    # for f in sorted(group3_files, key=natural_sort_key):
    #     print(f" - {f}")

# 実行部分
if __name__ == "__main__":
    # 入力フォルダと出力ファイルのパスを設定
    input_root_folder = "../data/tables_from_docx"  # あなたの環境に応じて変更してください
    output_combined_main_csv_file = os.path.join(input_root_folder, "combined_group1_main.csv")
    output_combined_annex_csv_file = os.path.join(input_root_folder, "combined_group1_annex.csv")

    print("--- CSVファイル処理を開始します ---")
    # 指定されたフォルダ内のCSVファイルを処理
    process_nested_csvs(input_root_folder, output_combined_main_csv_file, output_combined_annex_csv_file)
    print("--- CSVファイル処理が完了しました ---")
