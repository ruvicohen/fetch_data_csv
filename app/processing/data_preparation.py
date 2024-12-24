from app.utils.event_utils import (
    change_names_columns, fill_missing_columns,
    reorder_columns, merge_dataframes, extract_columns, convert_to_mongo_schema
)
from app.utils.files_utils import read_csv_file


def prepare_dataframe(file1, file2):
    df1 = read_csv_file(file1)
    df2 = read_csv_file(file2)

    df2 = change_names_columns(df2)
    df1 = extract_columns(df1)
    df2 = fill_missing_columns(df1, df2)
    df2 = reorder_columns(df1, df2)
    merged_df = merge_dataframes(df1, df2)

    return merged_df

def prepare_data_for_mongo(df):
    return [convert_to_mongo_schema(row) for _, row in df.iterrows()]
