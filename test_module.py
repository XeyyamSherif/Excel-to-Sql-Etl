'''
This is a test module
'''


import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Date, MetaData, Table, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import date
import math


excel_file_path = './excel/test_many.xlsx'
database_url = 'postgresql://postgres:1234@127.0.0.1:5432/testDB'

engine = create_engine(database_url)

Base = declarative_base()

class YourTable(Base):
    __tablename__ = 'test_table'

    id = Column(Integer, primary_key=True)
    Segment = Column(String, default=None)
    basa = Column(Integer, default=None)
    A_b = Column(String, default=None)
    test_decimal = Column(Float, default=None)
    Sales = Column(Integer, default=None)
    test_new_column = Column(String, default=None)

Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

df = pd.read_excel(excel_file_path)

for index, row in df.iterrows():

    for col in df.columns:
        if isinstance(row[col], (float, int)) and math.isnan(row[col]):
            row[col] = None

    record = YourTable(
        Segment=row.get('Segment', None),
        basa=row.get('basa', None),
        A_b=row.get('A b', None),
        test_decimal=row.get('test_decimal', None),
        Sales=row.get('Sales', None)
    )
    session.add(record)


session.commit()
session.close()
