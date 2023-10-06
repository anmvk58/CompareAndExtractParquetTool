from src.compare_module.Compare_object import Compare_Object_Fast
from src.compare_module.Compare_object import Compare_Object_Query
import pandas as pd

def compare_2_table_count(cof_1, cof_2):
    """
    :param cof_1:
    :type cof_1: Compare_Object_Fast
    :param cof_2:
    :type cof_2: Compare_Object_Fast
    :return:
    """
    df_1 = cof_1.get_data_check_count()
    df_1.columns = df_1.columns.str.lower()
    df_2 = cof_2.get_data_check_count()
    df_2.columns = df_2.columns.str.lower()
    df_diff = pd.concat([df_1, df_2]).drop_duplicates(keep=False)
    df_1['Location'] = cof_1.command.schema_table
    df_2['Location'] = cof_2.command.schema_table
    df_merge = pd.concat([df_1, df_2])

    if (df_diff.size == 0):
        return True, df_diff, df_merge
    else:
        return False, df_diff, df_merge

def compare_2_table_number(cof_1, cof_2):
    """
    :param cof_1:
    :type cof_1: Compare_Object_Fast
    :param cof_2:
    :type cof_2: Compare_Object_Fast
    :return:
    """
    df_1 = cof_1.get_data_check_number()
    df_1.columns = df_1.columns.str.lower()
    df_2 = cof_2.get_data_check_number()
    df_2.columns = df_2.columns.str.lower()
    df_diff = pd.concat([df_1, df_2]).drop_duplicates(keep=False)
    df_1['Location'] = cof_1.command.schema_table
    df_2['Location'] = cof_2.command.schema_table
    df_merge = pd.concat([df_1, df_2])

    if (df_diff.size == 0):
        return True, df_diff, df_merge
    else:
        return False, df_diff, df_merge

def compare_2_table_not_number(cof_1, cof_2):
    """
    :param cof_1:
    :type cof_1: Compare_Object_Fast
    :param cof_2:
    :type cof_2: Compare_Object_Fast
    :return:
    """
    df_1 = cof_1.get_data_check_not_number()
    df_1.columns = df_1.columns.str.lower()
    df_2 = cof_2.get_data_check_not_number()
    df_2.columns = df_2.columns.str.lower()
    df_diff = pd.concat([df_1, df_2]).drop_duplicates(keep=False)
    df_1['Location'] = cof_1.command.schema_table
    df_2['Location'] = cof_2.command.schema_table
    df_merge = pd.concat([df_1, df_2])

    if (df_diff.size == 0):
        return True, df_diff, df_merge
    else:
        return False, df_diff, df_merge

def compare_2_query(coq_1, coq_2):
    """
    :param coq_1:
    :type coq_1: Compare_Object_Query
    :param coq_2:
    :type coq_2: Compare_Object_Query
    :return:
    """
    try:
        df_1, total_col_1 = coq_1.get_data_after_process()
        df_2, total_col_2 = coq_2.get_data_after_process()
    except pd.io.sql.DatabaseError as e:
        raise Exception(e)

    message = ""

    if(total_col_1 != total_col_2):
        message= "FAIL: Số cột trả về của 2 truy vấn khác nhau !!!"
        return False, None, message
        # raise Exception("Số cột trả về của 2 truy vấn khác nhau !!!")
    elif(df_1.shape[0] != df_2.shape[0]):
        # raise Exception("Số lượng bản ghi trả về của 2 truy vấn khác nhau !!!")
        message = "FAIL: Số lượng bản ghi trả về của 2 truy vấn khác nhau !!!"
        df_diff = df_1.merge(df_2, indicator=True, how='outer')
        df_diff['_merge'] = df_diff['_merge'].replace(['left_only', 'right_only'], [coq_1.src_name, coq_2.src_name])
        first_column = df_diff.pop('_merge')
        df_diff.insert(0, '_Check', first_column)
        return False, df_diff, message
    else:
        # df_diff = df_1.compare(df_2, align_axis=0, keep_equal=True).rename(index={'self': coq_1.src_name, 'other': coq_2.src_name}, level=-1)
        df_diff = df_1.merge(df_2, indicator=True, how='outer')
        df_diff['_merge'] = df_diff['_merge'].replace(['left_only', 'right_only'], [coq_1.src_name, coq_2.src_name])
        df_diff.drop(df_diff.loc[df_diff['_merge'] == 'both'].index, inplace=True)
        first_column = df_diff.pop('_merge')
        df_diff.insert(0, '_Check', first_column)
        if (df_diff.size == 0):
            message = "PASS: 2 Truy vấn có kết quả giống nhau !"
            return True, df_diff, message
        else:
            message = "FAIL: 2 Truy vấn có kết quả khác nhau !"
            return False, df_diff, message

def compare_2_data_frame(df_1, df_2, src_name_1, src_name_2 ):
    """
    :param df_1:
    :type df_1: pandas.core.frame.DataFrame
    :param df_2:
    :type df_2: pandas.core.frame.DataFrame
    :return:
    """
    message = ""
    # Khac column count
    if(df_1.shape[1] != df_2.shape[1]):
        message = "FAIL: Số cột của 2 batch khác nhau !!!"
        print(message)
        return False, None, message
    # Khac row count
    elif(df_1.shape[0] != df_2.shape[0]):
        message = "FAIL: Số dòng của 2 batch khác nhau !!!"
        df_diff = df_1.merge(df_2, indicator=True, how='outer')
        df_diff['_merge'] = df_diff['_merge'].replace(['left_only', 'right_only'], [src_name_1, src_name_2])
        first_column = df_diff.pop('_merge')
        df_diff.insert(0, '_Check', first_column)
        print(message)
        return False, df_diff, message
    else:
        # df_diff = df_1.compare(df_2, align_axis=0, keep_equal=True).rename(index={'self': src_name_1, 'other': src_name_2}, level=-1)
        df_diff = df_1.merge(df_2, indicator=True, how='outer')
        df_diff['_merge'] = df_diff['_merge'].replace(['left_only', 'right_only'], [src_name_1, src_name_2])
        df_diff.drop(df_diff.loc[df_diff['_merge'] == 'both'].index, inplace=True)
        first_column = df_diff.pop('_merge')
        df_diff.insert(0, '_Check', first_column)
        if (df_diff.size == 0):
            message = "PASS: Batch giống nhau !"
            print(message)
            return True, df_diff, message
        else:
            message = "FAIL: Batch khác nhau !"
            print(message)
            return False, df_diff, message


