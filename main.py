import pandas as pd
from sqlalchemy import create_engine, text, String
from util_tool import get_data_type_info


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
          f'ALTER TABLE finance_sample ADD COLUMN "{column}" {get_data_type_info(dtype)};'
      )
      print(query)
      alter_query = query
      connection.execute(alter_query)
      connection.commit()


def update_dataframe_with_missing_columns(dataframe, missing_columns):
  for column in missing_columns:
    dataframe[column] = None
  return dataframe


def write_to_sql(dataframe, table_name, engine):
  dataframe.to_sql(table_name, engine, index=False, if_exists='append')


def main():
  file_path = './excel/test_decimal.xlsx'
  table_name = 'finance_sample'
  DATABASE_URL = 'postgresql://postgres:1234@127.0.0.1:5432/testDB'  #burada oz db url - nizi yazin
  # DATABASE_URL = 'postgres://pass@host/db_name'   diger url ile ishlemese bu url ile yoxlayin


  engine = create_engine(DATABASE_URL)
  df_finance = read_excel(file_path)
  try:
    if check_table_exists(engine, table_name):
      existing_columns = get_existing_columns(engine, table_name)
      missing_columns_in_excel = set(existing_columns) - set(df_finance.columns)
      missing_columns_in_sql = list(
          set(df_finance.columns) - set(existing_columns))

      if missing_columns_in_excel:
        df_finance = update_dataframe_with_missing_columns(
            df_finance, missing_columns_in_excel)

      if missing_columns_in_sql:
        add_missing_columns(engine, table_name, df_finance,
                            missing_columns_in_sql)
    write_to_sql(df_finance, table_name, engine)
  except Exception as err:
    print(f'Error occured: {str(e)}')
  finally:
      engine.dispose()


if __name__ == "__main__":
  main()