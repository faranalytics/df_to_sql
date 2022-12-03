
import hashlib
import typing
import numpy as np
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import orm
import alembic as al
from alembic import migration
from alembic import operations
from logger import logger
import decimal


date_regexes = [r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}([+-]\d{2}:\d{2}|Z)?$']
number_regexes = [r'^([1-9][0-9]+|[0-9])(\.[0-9]+$|$)']


def is_datetime(ds:pd.Series) -> bool:

    try:
        assert ds.dropna().shape[0] != 0
        assert all([ds.str.match(regex).all() for regex in date_regexes])
        return True
    except:
        return False


def is_numeric(ds:pd.Series) -> bool:

    try:
        assert ds.dropna().shape[0] != 0
        assert all([ds.str.match(regex).all() for regex in number_regexes])
        return True
    except:
        return False


def is_string(ds:pd.Series) -> bool:

    return ds.dropna().shape[0] != 0


def get_sql_type(ds) -> sa.sql.visitors.Traversible:
        
    if is_numeric(ds):
        
        whole = ds.str.extract(r'^([0-9]+)', expand=False).dropna().str.count(r'[0-9]').max()

        fractional = ds.str.extract(r'(?<=\.)([0-9]+)', expand=False).dropna().str.count(r'[0-9]').max()

        fractional = 0 if np.isnan(fractional) else fractional

        return sa.types.Numeric(precision=whole + fractional, scale=fractional)

    elif is_datetime(ds):

        return sa.types.DateTime()

    elif is_string(ds):

        length = ds.dropna().str.len().max()

        return sa.types.String(length=length)

    else:
        return sa.types.Numeric(precision=1, scale=0)


def get_tables(conn:sa.engine.Connection) -> dict[str, sa.schema.Table]:
    metadata_obj = sa.MetaData(bind=conn)
    metadata_obj.reflect(bind=conn)
    return metadata_obj.tables


def get_table(table_name:str, conn:sa.engine.Connection) -> sa.schema.Table:
    tables = get_tables(conn=conn)
    return tables[table_name]


def get_table_names(conn:sa.engine.Connection) -> list[str]:
    tables = get_tables(conn=conn)
    return list(tables.keys())


def get_columns(table_name:str, conn:sa.engine.Connection) -> list[sa.schema.Column]:
    metadata_obj = sa.MetaData(bind=conn)
    metadata_obj.reflect(bind=conn, only=[table_name])
    table:sa.schema.Table = metadata_obj.tables[table_name]
    return table.columns


def get_column(table_name:str, column_name:str, conn:sa.engine.Connection) -> sa.schema.Column:
    columns = get_columns(table_name=table_name, conn=conn)
    return columns[column_name]


def get_column_names(table_name:str, conn:sa.engine.Connection) -> list[str]:
    return [column.name for column in get_columns(table_name=table_name, conn=conn)]


def modify_column(table_name:str, vendor_column:sa.schema.Column, migration_column:sa.schema.Column, op:operations.Operations) -> None:

    try:
        trans = op.migration_context.connection.begin()
        
        op.add_column(
        table_name=table_name, 
        column=migration_column
        )

        vendor_table = get_table(table_name=table_name, conn=op.migration_context.connection)
        
        cast = sa.cast(vendor_column, migration_column.type)

        op.migration_context.connection.execute(vendor_table.update().values(**{migration_column.name:cast}))
        
        op.drop_column(table_name=table_name, column_name=vendor_column.name)

        op.alter_column(
        table_name=table_name, 
        column_name=migration_column.name, 
        new_column_name=vendor_column.name
        )

        trans.commit()
    except BaseException as e:
        logger.error(f"Rollback attempt to change the type of the column named {vendor_column.name} from {vendor_column.type} to {migration_column.type}.")
        trans.rollback()
        raise e
    finally:
        if migration_column.name in get_column_names(table_name=table_name, conn=op.migration_context.connection):
            op.drop_column(table_name=table_name, column_name=migration_column.name)
        

def get_migration_type(ds:pd.Series, vendor_column:sa.schema.Column, op:operations.Operations) -> typing.Union[sa.sql.visitors.Traversible, None]:

    if is_numeric(ds) and isinstance(vendor_column.type, sa.types.Numeric):

        import_type_whole = ds.str.extract(r'^([0-9]+)', expand=False).dropna().str.count(r'[0-9]').max()
        import_type_whole = 1 if np.isnan(import_type_whole) else import_type_whole
        
        import_type_fractional = ds.str.extract(r'(?<=\.)([0-9]+)', expand=False).dropna().str.count(r'[0-9]').max()
        import_type_fractional = 0 if np.isnan(import_type_fractional) else import_type_fractional
        
        vendor_type_whole = vendor_column.type.precision-vendor_column.type.scale

        if import_type_whole > vendor_type_whole or import_type_fractional > vendor_column.type.scale:

            max_whole = max(import_type_whole, vendor_type_whole)
            max_fractional = max(import_type_fractional, vendor_column.type.scale)
            return sa.types.Numeric(precision=max_whole + max_fractional, scale=max_fractional)

    elif is_datetime(ds) and isinstance(vendor_column.type, sa.types.DateTime):
        return
    elif is_string(ds):
        import_type_length = ds.dropna().str.len().max()

        if isinstance(vendor_column.type, sa.types.String):
            if import_type_length > vendor_column.type.length:
                return sa.types.String(length=import_type_length)
            else:
                return
        else:
            select = sa.select(sa.func.max(sa.func.length(sa.type_coerce(vendor_column, sa.types.String()))))
            vendor_type_length = op.migration_context.connection.execute(select).first()[0]
            if vendor_type_length is None:
                vendor_type_length = 0
            return sa.types.String(length=max(import_type_length, vendor_type_length))
    elif ds.isna().all():
        return
    else:
        raise Exception(f"Unhandled type for the column named {vendor_column} of the type {vendor_column.type}.")


def df_to_sql(df:pd.DataFrame, table_name:str, schema:str, engine:sa.engine.Engine) -> None:

     try:
          logger.info(f"Begin sql import into the table named {table_name}.")

          conn:sa.engine.Connection = engine.connect()
          op = operations.Operations(migration.MigrationContext.configure(conn))
          
          if table_name.isupper():
               table_name = table_name.lower()
               #  SA table names are lowercase if the name is all one case; hence, convert the table_name to lowercase.

          assert not df.columns.duplicated().any()
          
          df.columns = [column_name.lower() if column_name.isupper() else column_name for column_name in df.columns]

          if table_name not in get_table_names(conn=conn):
               logger.debug(f"Create new table for the table named {table_name}.")
               op.create_table(
                    table_name, 
                    *[sa.schema.Column(import_column_name, get_sql_type(df[import_column_name]), nullable=True) for import_column_name in df.columns]
                    )
          else:
               #  Begin migration.
               for import_column_name in df.columns:
                    #  The import column is already in the vendor table; hence, just add the column - a type change isn't needed.
                    if import_column_name not in get_column_names(table_name=table_name, conn=conn):
                         logger.debug(f"Create the column named {import_column_name} in the table named {table_name}")
                         op.add_column(
                              table_name=table_name, 
                              column=sa.schema.Column(import_column_name, get_sql_type(df[import_column_name]), nullable=True)
                              )
                    else:
                         #  The import column already exists in the table; hence, determine if a type change is needed.
                         vendor_column = get_column(table_name=table_name, column_name=import_column_name, conn=conn)
                         migration_type = get_migration_type(ds=df[import_column_name], vendor_column=vendor_column, op=op)
                         if migration_type is not None:

                              column_names = get_column_names(table_name=table_name, conn=op.migration_context.connection)
                              migration_column_name = hashlib.md5(''.join(column_names).encode()).hexdigest()[0:16]
                              
                              if migration_column_name in column_names:
                                   raise Exception(f"The migration column name {migration_column_name} is in the vendor table named {table_name}.")
                              
                              migration_column = sa.schema.Column(
                              name=migration_column_name,
                              type_=migration_type,
                              nullable=True
                              )

                              logger.debug(f"Change the type of the column named {vendor_column.name} in the table named {table_name} from {repr(vendor_column.type)} to {repr(migration_column.type)}.")
                              
                              modify_column(table_name=table_name, vendor_column=vendor_column, migration_column=migration_column, op=op)

          vendor_table = get_table(table_name=table_name, conn=conn)
          
          for vendor_column in vendor_table.columns:
          #  The migration of the vendor table has completed; hence, type the dataframe according to the types in the vendor table.

               if vendor_column.name in df.columns:
                    
                    if isinstance(vendor_column.type, sa.types.Numeric):

                         df.loc[df[vendor_column.name].notna(), vendor_column.name] = df.loc[df[vendor_column.name].notna(), vendor_column.name].apply(decimal.Decimal)

                    elif isinstance(vendor_column.type, sa.types.DateTime):

                         df.loc[df[vendor_column.name].notna(), vendor_column.name] = pd.to_datetime(df.loc[df[vendor_column.name].notna(), vendor_column.name], utc=True)
          
          df = df.drop_duplicates()
               
          records = df.replace({np.nan:None}).to_dict('records')
          #  SA interprets None as NULL.

          if len(records) > 0:
               try:
                    trans = conn.begin()

                    temp_table_name = hashlib.md5(table_name.encode()).hexdigest()[0:16]
                    if temp_table_name in get_table_names(conn=conn):
                         raise Exception(f"The table named {temp_table_name} is already present.")

                    logger.debug(f"Create the temporary table named {temp_table_name}.")

                    temp_table_columns = [sa.schema.Column(column.name, column.type, nullable=column.nullable) for column in vendor_table.columns]
                    temp_table = op.create_table(temp_table_name, *temp_table_columns)
                    #  The columns that comprise temp_table_columns will form an auto-generated index; hence it is not needed to create an Index.

                    logger.debug(f"Insert records into the temporary table named {temp_table_name}.")

                    op.bulk_insert(temp_table, records)

                    logger.debug(f"Select and insert unique records into the table named {table_name}.")

                    select:sa.sql.expression.CompoundSelect = sa.sql.expression.except_(sa.select(temp_table), sa.select(vendor_table))
                    conn.execute(sa.insert(vendor_table).from_select([*vendor_table.columns], select))
                    
                    trans.commit()
               except BaseException as e:
                    logger.error(f"Rollback attempt to insert new records into {table_name}.")
                    trans.rollback()
                    raise e
               finally:
                    if temp_table_name in get_table_names(conn=conn):
                         logger.debug(f"Drop the temporary table named {temp_table_name}.")
                         op.drop_table(temp_table_name, schema=schema)
     finally:
          conn.close()
          logger.info(f"End sql import procedure into the table named {table_name}.")

