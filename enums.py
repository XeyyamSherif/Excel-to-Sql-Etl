from enum import Enum


class DataTypePostgre(Enum):
  int = "INT"
  float = "FLOAT"
  str = "VARCHAR"
  bool = "BOOL"
  datetime = "TIMESTAMP"
  numeric = "NUMERIC"


