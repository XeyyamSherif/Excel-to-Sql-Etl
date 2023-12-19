import pandas as pd
from sqlalchemy import create_engine, text
from enums import DataTypePostgre


def get_data_type_info(dtype):
  if pd.api.types.is_string_dtype(dtype):
    return DataTypePostgre.str.value
  elif pd.api.types.is_integer_dtype(dtype):
    return DataTypePostgre.int.value
  elif pd.api.types.is_datetime64_any_dtype(dtype):
    return DataTypePostgre.datetime.value
  elif pd.api.types.is_numeric_dtype(dtype):
    return DataTypePostgre.int.value
  else:
    return DataTypePostgre.str.value
