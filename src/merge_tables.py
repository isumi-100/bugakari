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
def handle_special_group1_case(df_original, file_path, expected_base_columns, name_aliases, final_group1_output_columns):
    """
    特定の列構造を持つDataFrameをGroup1形式に変換します。
    この関数は、'単位'と'備考'の間に単一の、'所要量'ではない列が存在する場合に適用されます。
    その列名を'作業名'にマージし、列を'所要量'にリネームします。
    また、'名称'列が'細目'として存在する場合も処理します。

    Args:
        df_original (pd.DataFrame): 処理対象の元のDataFrame。
        file_path (str): 処理中のファイルのパス（ログ出力用）。
        expected_base_columns (list): 期待される基本列名のリスト。
        name_aliases (list): '名称'列として許容される列名のリスト（例: ['名称', '細目']）。
        final_group1_output_columns (list): 最終的にGroup1として出力されるべき列の厳密なリスト。

    Returns:
        tuple: (変換されたDataFrame, エラーメッセージ)。
               変換が成功した場合は(pd.DataFrame, None)、失敗した場合は(None, str)を返します。
    """
    df = df_original.copy() # オリジナルCSVは編集しないため、DataFrameのコピーを操作
    col_list = df.columns.tolist()

    # まず、name_aliasesに含まれる列があれば、それを'名称'にリネーム
    renamed = False
    for alias in name_aliases:
        if alias in col_list and alias != '名称':
            # Check if '名称' is already present
            if '名称' in col_list:
                return None, f"'{alias}'と'名称'の両方が存在します。"
            df = df.rename(columns={alias: '名称'})
            col_list = df.columns.tolist() # 列名リストを更新
            renamed = True
            break # 最初のマッチしたエイリアスのみ処理

    # 期待される列のプレフィックスとサフィックスを定義
    # '名称'がエイリアスから来る可能性があるので、expected_base_columnsから動的に生成
    # e.g., ['表', '作業名', '名称', '摘要', '単位', '所要量', '備考']
    expected_prefix_cols = ['表', '作業名', '名称', '摘要', '単位']
    expected_suffix_col = '備考'

    # 1. 最小限の列数と主要な列の存在チェック
    if not all(col in col_list for col in ['表', '作業名', '名称', '単位', '備考']):
        return None, "必要な列('表', '作業名', '名称', '単位', '備考')が見つかりません。"

    # 2. プレフィックス列が期待されるものと一致するかチェック（'名称'は動的に判定後）
    # 列順の厳密なチェック: '表', '作業名', (名称 or 細目), '摘要', '単位'
    # 現在の列リストから、expected_prefix_colsに対応する部分を抽出して比較
    
    # '名称'列の実際の位置を探す
    name_col_idx = -1
    for alias in name_aliases:
        if alias in col_list:
            name_col_idx = col_list.index(alias)
            break
    
    if name_col_idx == -1: # '名称'またはエイリアスが見つからない
        return None, "想定される名称列 ('名称' または '細目') が見つかりません。"

    # 期待されるプレフィックスの順序を動的に構築
    # '表', '作業名', '名称', '摘要', '単位' の順であるか確認
    if not (len(col_list) > name_col_idx + 2 and
            col_list[0] == '表' and
            col_list[1] == '作業名' and
            col_list[name_col_idx] == '名称' and # 既にリネーム済み
            col_list[name_col_idx + 1] == '摘要' and
            col_list[name_col_idx + 2] == '単位'):
        return None, "プレフィックス列の順序が期待されるものと一致しません。"


    # 3. '備考'列が期待される位置にあるかチェック
    # '名称'を基準に相対位置を計算
    if not ('備考' in col_list):
        return None, "'備考'列が見つかりません。"
    
    idx_unit = col_list.index('単位')
    idx_remarks = col_list.index('備考')

    if not (idx_remarks > idx_unit): # '備考'が'単位'より後にあるか
        return None, "'備考'列が'単位'列より前にあります。"

    # 4. インデックス5の列が'所要量'ではないかチェック (単一列展開の条件)
    # idx_unit + 1 の位置が単一列の候補
    col_to_merge_name_idx = idx_unit + 1
    if col_to_merge_name_idx >= idx_remarks: # '単位'と'備考'の間に列がない、または複数列の場合もここで弾かれる
        return None, "'単位'と'備考'の間に単一の列がありません。"
    
    col_to_merge_name = col_list[col_to_merge_name_idx]

    if col_to_merge_name == '所要量':
        return None, "インデックス5の列は既に'所要量'です（特殊ケースではありません）。"
    
    # 5. '単位'と'備考'の間に単一の列があるか最終チェック
    if not (idx_remarks - idx_unit - 1 == 1):
        return None, "'単位'と'備考'の間に単一の列がありません。"


    # 全ての条件を満たした場合、変換処理を実行
    if renamed:
        print(f"   > '細目'列を'名称'にリネームしました。")
    print(f"   > 特殊なGroup1形式の変換を適用中 (単一列展開): {os.path.basename(file_path)}")

    # 該当する列名を作業列名のセルに文字列マージ
    # 変更点: 列名ではなく、その列の値をマージ
    df['作業名'] = df['作業名'].astype(str) + df[col_to_merge_name].astype(str) 

    # 該当する列名を'所要量'にリネーム
    df = df.rename(columns={col_to_merge_name: '所要量'})

    # 最終的な列の選択と順序付け
    # final_group1_output_columns に含まれない列はここで削除される
    df_final = df.reindex(columns=final_group1_output_columns, fill_value='')

    return df_final, None # 変換されたDataFrameとエラーなしを返す

# 複数列を展開して行に変換する特殊なGroup1形式のCSVを処理する関数
def handle_multi_column_expansion_case(df_original, file_path, expected_base_columns, name_aliases, final_group1_output_columns):
    """
    '単位'と'備考'の間に複数の所要量相当の列があるDataFrameを、
    それらの列を展開して行に変換し、'作業名'に列名をマージする形式に変換します。
    また、'名称'列が'細目'として存在する場合も処理します。

    Args:
        df_original (pd.DataFrame): 処理対象の元のDataFrame。
        file_path (str): 処理中のファイルのパス（ログ出力用）。
        expected_base_columns (list): 期待される基本列名のリスト。
        name_aliases (list): '名称'列として許容される列名のリスト（例: ['名称', '細目']）。
        final_group1_output_columns (list): 最終的にGroup1として出力されるべき列の厳密なリスト。

    Returns:
        tuple: (変換されたDataFrame, エラーメッセージ)。
               変換が成功した場合は(pd.DataFrame, None)、失敗した場合は(None, str)を返します。
    """
    df = df_original.copy()
    col_list = df.columns.tolist()

    # まず、name_aliasesに含まれる列があれば、それを'名称'にリネーム
    renamed = False
    for alias in name_aliases:
        if alias in col_list and alias != '名称':
            if '名称' in col_list:
                return None, f"'{alias}'と'名称'の両方が存在します。"
            df = df.rename(columns={alias: '名称'})
            col_list = df.columns.tolist() # 列名リストを更新
            renamed = True
            break # 最初のマッチしたエイリアスのみ処理

    # 期待される基本列のプレフィックスとサフィックス
    expected_prefix_cols = ['表', '作業名', '名称', '摘要', '単位']
    expected_suffix_col = '備考'

    # 1. '所要量'列が既に存在しないかチェック
    if '所要量' in col_list:
        return None, "DataFrameに既に'所要量'列が存在します。"

    # 2. 必要な主要列の存在と列数のチェック
    if not all(col in col_list for col in ['表', '作業名', '名称', '単位', '備考']):
        return None, "必要な列('表', '作業名', '名称', '単位', '備考')が見つかりません。"

    try:
        idx_unit = col_list.index('単位')
        idx_remarks = col_list.index('備考')
    except ValueError:
        return None, "'単位'または'備考'列のインデックスが見つかりません。"

    # 3. '単位'が'備考'より前にあり、その間に複数の列があるかチェック (複数列展開の条件)
    if not (idx_unit < idx_remarks - 1 and (idx_remarks - idx_unit - 1) > 1):
        return None, "'単位'と'備考'の間に複数の列がありません。"

    # 4. プレフィックス列が期待されるものと一致するかチェック（'名称'は動的に判定後）
    # '名称'列の実際の位置を探す
    name_col_idx = -1
    for alias in name_aliases:
        if alias in col_list:
            name_col_idx = col_list.index(alias)
            break
    
    if name_col_idx == -1: # '名称'またはエイリアスが見つからない
        return None, "想定される名称列 ('名称' または '細目') が見つかりません。"

    # 期待されるプレフィックスの順序を動的に構築
    # '表', '作業名', '名称', '摘要', '単位' の順であるか確認
    if not (len(col_list) > name_col_idx + 2 and
            col_list[0] == '表' and
            col_list[1] == '作業名' and
            col_list[name_col_idx] == '名称' and # 既にリネーム済み
            col_list[name_col_idx + 1] == '摘要' and
            col_list[name_col_idx + 2] == '単位'):
        return None, "プレフィックス列の順序が期待されるものと一致しません。"

    if renamed:
        print(f"   > '細目'列を'名称'にリネームしました。")
    print(f"   > 特殊なGroup1形式の変換を適用中 (複数列展開): {os.path.basename(file_path)}")

    # 展開対象となる列（'単位'と'備考'の間の列）を特定
    columns_to_melt = col_list[idx_unit + 1 : idx_remarks]
    
    # ID列として残す列（展開しない列）
    # '注'列が最終的に必要な場合は、ここでid_varsに含める必要がある
    # ただし、最終的にfinal_group1_output_columnsで選択するので、ここで厳密に定義する必要はない
    # '単位'までと'備考'以降の列（ただし、備考以降の列は最終的にfinal_group1_output_columnsでフィルタリングされる）
    id_vars = col_list[0:idx_unit+1] + col_list[idx_remarks:] 

    # melt（展開）処理
    df_melted = df.melt(id_vars=id_vars,
                         value_vars=columns_to_melt,
                         var_name='展開列名', # 一時的に展開元の列名を保持する新しい列名
                         value_name='所要量') # 展開後の数値が格納される列名

    # '作業名'に展開元の列名を結合
    df_melted['作業名'] = df_melted['作業名'].astype(str) + df_melted['展開列名'].astype(str)
    
    # 不要になった一時列を削除
    df_melted = df_melted.drop(columns=['展開列名'])

    # 最終的な列の選択と順序付け
    # final_group1_output_columns に含まれない列はここで削除される
    df_final = df_melted.reindex(columns=final_group1_output_columns, fill_value='')

    return df_final, None # 変換されたDataFrameとエラーなしを返す


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

    # 期待される基本列の定義（変換前のチェック用）
    expected_base_columns = ['表', '作業名', '名称', '摘要', '単位', '所要量','備考']
    # '名称'列として許容されるエイリアス
    name_aliases = ['名称', '細目']
    # 最終的にGroup1として出力されるべき列の厳密なリスト
    # '注'列も最終出力に含める
    final_group1_output_columns = ['表', '作業名', '名称', '摘要', '単位', '所要量', '備考', '注']

    # Group2の最小限の必須列の定義
    # '名称'の代わりに'細目'も許容するよう調整
    min_group2_columns = set(['摘要', '単位', '所要量', '備考'])
    
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
            # 1. 厳密なGroup1の列構造を試す (名称エイリアスを考慮)
            # まず、名称エイリアス列があればリネームして正規化
            df_for_check = df.copy()
            initial_col_list = df_for_check.columns.tolist()
            name_col_renamed = False
            for alias in name_aliases:
                if alias in initial_col_list and alias != '名称':
                    if '名称' in initial_col_list:
                        continue # '名称'とエイリアスが両方ある場合はスキップ (このケースはhandle_special_group1_case内でエラーになる)
                    df_for_check = df_for_check.rename(columns={alias: '名称'})
                    name_col_renamed = True
                    break
            
            # リネーム後のカラムリストでチェック
            current_cols_after_rename = df_for_check.columns.tolist()

            # 厳密なGroup1チェックも'名称'のエイリアスを考慮するように調整
            # '名称'が必須位置にあり、その後の列順が正しいかを確認
            if '名称' in current_cols_after_rename:
                idx_name = current_cols_after_rename.index('名称')
                # ここでのexpected_base_columnsは、所要量が含まれる厳密な7列を想定
                # '注'列は後のreindexで追加されるため、ここでは考慮しない
                strict_group1_cols_for_check = ['表', '作業名', '名称', '摘要', '単位', '所要量', '備考']
                
                # 現在のDataFrameの列が、strict_group1_cols_for_checkの順序と一致するかをチェック
                # ただし、元のDataFrameには'所要量'が存在しない場合もあるため、
                # '単位'の次が'備考'で、その間に1つまたは複数の列があることを確認するロジックの方が適切
                
                # 簡略化された厳密なチェック: 必要な基本列が全て存在し、かつ順序が正しいか
                # '表', '作業名', '名称', '摘要', '単位', '所要量', '備考'
                # ここでは、df_for_checkの列がこれらの列と完全に一致するか、またはこれらをプレフィックスとして持つかを確認
                
                # まず、必要な列が全て存在するか
                if all(col in current_cols_after_rename for col in strict_group1_cols_for_check):
                    # そして、それらの列が期待される順序で並んでいるか
                    # '表', '作業名', '名称', '摘要', '単位' の順序
                    idx_table = current_cols_after_rename.index('表')
                    idx_workname = current_cols_after_rename.index('作業名')
                    idx_abstract = current_cols_after_rename.index('摘要')
                    idx_unit_strict = current_cols_after_rename.index('単位')
                    idx_required = current_cols_after_rename.index('所要量')
                    idx_remarks_strict = current_cols_after_rename.index('備考')

                    if (idx_table == 0 and idx_workname == 1 and idx_name == 2 and 
                        idx_abstract == 3 and idx_unit_strict == 4 and 
                        idx_required == 5 and idx_remarks_strict == 6):
                        
                        if current_table_id_prefix and is_valid_table_id_pattern(df_for_check['表'], current_table_id_prefix):
                            print(f"Group1 ({current_table_id_prefix} - 厳密な一致{' (細目→名称リネーム)' if name_col_renamed else ''}): {file_path}")
                            target_dfs_list = group1_main_dfs if current_table_id_prefix == '表' else group1_annex_dfs
                            
                            # 厳密な一致の場合も最終出力列に合わせる
                            final_df_strict = df_for_check.reindex(columns=final_group1_output_columns, fill_value='')
                            target_dfs_list.append((final_df_strict, file_path))
                            is_classified_as_group1 = True
            
            # 2. 特殊なGroup1形式 (単一列の変換) を試す
            if not is_classified_as_group1:
                modified_df, error_msg = handle_special_group1_case(df, file_path, expected_base_columns, name_aliases, final_group1_output_columns)
                if modified_df is not None: # 構造変換が成功した場合
                    # 変換後のDataFrameの'表'列が正しいパターンに合致するかチェック
                    if current_table_id_prefix and is_valid_table_id_pattern(modified_df['表'], current_table_id_prefix):
                        print(f"Group1 ({current_table_id_prefix} - 単一列変換): {file_path}")
                        target_dfs_list = group1_main_dfs if current_table_id_prefix == '表' else group1_annex_dfs
                        target_dfs_list.append((modified_df, file_path))
                        is_classified_as_group1 = True
            
            # 3. 特殊なGroup1形式 (複数列の展開) を試す
            if not is_classified_as_group1:
                modified_df, error_msg = handle_multi_column_expansion_case(df, file_path, expected_base_columns, name_aliases, final_group1_output_columns)
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
            # '名称'または'細目'が存在するかを確認
            has_name_or_saimoku = any(alias in col_list for alias in name_aliases)
            
            # 必須列に加えて、'名称'または'細目'が一つだけ不足しているパターン
            # min_group2_columns には '名称' を含まないようにし、has_name_or_saimoku で別途チェック
            present_cols_set = set(col_list)
            
            # Group2は厳密にはGroup1の条件に合わないが、'名称'/'細目'およびその他の主要列がほぼ揃っているもの
            # ここでは、'名称' (または細目) があり、かつ min_group2_columns (摘要, 単位, 所要量, 備考) が全てある場合
            # をGroup2とする
            if has_name_or_saimoku and min_group2_columns.issubset(present_cols_set):
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
    for f in sorted(group3_files, key=natural_sort_key):
        print(f" - {f}")

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