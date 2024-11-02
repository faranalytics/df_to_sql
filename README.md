# DataFrame To SQL

**NB** This project is deprecated.  I think there are better ways to accomplish this task.  However, there are alot of useful examples of working with SQL Alchemy in the code of this project; hence, I want to maintain this for reference.

DataFrame to SQL facilitates importing Pandas DataFrames into SQL databases.  It automates table and type migrations and deduplication of records.

## Usage

```py
from df_to_sql import DFToSQL
from df_to_sql import date_regexes
from df_to_sql import number_regexes

date_regexes.append(r'^[0-9]{4}-[0-9]{2}-[0-9]{2}$')

dts = DFToSQL(date_regexes=date_regexes)

if 'sqlite.db' in os.listdir('.'): os.remove('./sqlite.db')

sqlite3.connect('sqlite.db')

sl_engine = sa.create_engine('sqlite:///sqlite.db')

for _ in range(0, 10):

    df = pd.util.testing.makeMixedDataFrame()

    dts.integrate(df=df, table_name='table_name', schema='main', engine=sl_engine)

sl_engine = sa.create_engine('sqlite:///sqlite.db')

df = pd.read_sql_table('table_name', con=sl_engine)

print(df.dtypes)

print(df.shape)

```
The imported table has these properties:

```
print(df.dtypes)
a             int64
b             int64
c            object
d    datetime64[ns]

print(df.shape)
dtype: object
(5, 4)
```