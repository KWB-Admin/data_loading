from load_functions import *
import polars as pl


if __name__ == "__main__":
    dbname = ""
    table_name = ""
    connection = get_pg_connecter(dbname)
    columns_and_dtypes = get_columns_and_dtypes(connection, table_name)
    data = pl.read_csv(r"")
    for items in columns_and_dtypes:
        col = items[0]
        dtype = items[1]
        if dtype == "integer":
            data = data.with_columns(pl.col(col).cast(pl.Int16))
        elif dtype == "text":
            data = data.with_columns(pl.col(col).cast(pl.String))
        elif dtype in ["double precision", "real"]:
            data = data.with_columns(pl.col(col).cast(pl.Float64))
        elif dtype == "date":
            data = data.with_columns(
                pl.col(col).str.strptime(pl.Date, format="%m/%d/%Y")
            )

    write_data_to_tables(data.to_numpy(), dbname, table_name)
