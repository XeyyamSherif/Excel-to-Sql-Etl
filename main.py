import pandas as pd
import logging
from sqlalchemy import create_engine, text, String
from util_tool import get_data_type_info, fix_schema_of_sql_table
from table_schema import main_schema
logging_format = "[%(levelname)s] %(asctime)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=logging_format)
logger = logging.getLogger()


def read_excel(file_path):
  return pd.read_excel(file_path)


def check_table_exists(engine, table_name):
  query = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}');"
  return pd.read_sql(query, engine)['exists'].iloc[0]


def get_existing_columns(engine, table_name):
  query = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}';"
  result_df = pd.read_sql(query, engine)
  columns_and_types = result_df.set_index('column_name')['data_type'].to_dict()
  return columns_and_types


def add_missing_columns(engine, table_name, dataframe, columns):
  data_types = dataframe[columns].dtypes
  with engine.connect() as connection:
    for column, dtype in data_types.items():
      query = text(
          f'ALTER TABLE {table_name} ADD COLUMN "{column}" {get_data_type_info(dtype)};'
      )
      logger.info(f"Executing query: {query}")
      connection.execute(query)
      connection.commit()


def update_dataframe_with_missing_columns(dataframe, missing_columns):
  for column in missing_columns:
    dataframe[column] = None
  return dataframe


def write_to_sql(dataframe, table_name, engine):
  dataframe.to_sql(table_name, engine, index=False, if_exists='append')
  logger.info(f'DataFrame added to table succesfully')


def main():
  file_path = './excel/financial_sample_main.xlsx'
  table_name = 'finance_sample'
  DATABASE_URL = 'postgresql://postgres:1234@127.0.0.1:5432/testDB'  #burada oz db url - nizi yazin
  # DATABASE_URL = 'postgres://pass@host/db_name'   diger url ile ishlemese bu url ile yoxlayin

  engine = create_engine(DATABASE_URL)
  df_finance = read_excel(file_path)
  try:
    fixed_df = fix_schema_of_sql_table(df_finance, main_schema)
    if not fixed_df.empty:
      if check_table_exists(engine, table_name):
        existing_columns = get_existing_columns(engine, table_name)
        missing_columns_in_excel = set(existing_columns) - set(fixed_df.columns)
        missing_columns_in_sql = list(
            set(fixed_df.columns) - set(existing_columns))

        if missing_columns_in_excel:
          fixed_df = update_dataframe_with_missing_columns(
              fixed_df, missing_columns_in_excel)

        if missing_columns_in_sql:
          add_missing_columns(engine, table_name, fixed_df,
                              missing_columns_in_sql)
      write_to_sql(fixed_df, table_name, engine)
    else:
      logger.info('No data to add')
  except Exception as err:
      logger.error(f'Error occurred: {str(err)}')
  finally:
      engine.dispose()




if __name__ == "__main__":
  main()
