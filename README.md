# df_to_sql

DataFrame to SQL facilitates importing Pandas DataFrames into SQL databases.  It automates table and type migrations and deduplication of records.

## Usage

```py
sqlite3.connect('sqlite.db')

sl_engine = sa.create_engine('sqlite:///sqlite.db')

for _ in range(0, 100):

    df = pd.util.testing.makeMixedDataFrame()

    df_to_sql(df=df, table_name='table_name', schema='main', engine=sl_engine)

sl_engine = sa.create_engine('sqlite:///sqlite.db')

df = pd.read_sql_table('table_name', con=sl_engine)

```
The resultig table has these properties:

```
print(df.dtypes)
a     int64
b     int64
c    object
d    object

print(df.shape)
dtype: object
(5, 4)
```

## Install
```bash
pip install df_to_sql
```