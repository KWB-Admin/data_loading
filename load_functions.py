import psycopg2 as pg
from psycopg2 import sql
from datetime import datetime

user, host, password = ["", "", ""]


def write_data_to_tables(data, db_name: str, table: str, prim_key: str = "None"):
    con = get_pg_connecter(db_name=db_name)
    check_tables_exists(con, table=table)
    columns_and_dtypes = get_columns_and_dtypes(con, table)
    try:
        cur = con.cursor()
        for row in data:
            query = build_insert_query(
                row,
                table=table,
                columns_and_dtypes=columns_and_dtypes,
                prim_key=prim_key,
            )
            cur.execute(query)
        cur.close()
        con.close()
        print(f"Data was successfully written to {table} table in {db_name} database")
    except:
        con.close()
        print("Data did not get written")


def build_insert_query(data: list, table: str, columns_and_dtypes: dict, prim_key: str):
    col_names = sql.SQL(", ").join(sql.Identifier(col[0]) for col in columns_and_dtypes)
    values = sql.SQL(" , ").join(sql.Literal(val) for val in data)
    if prim_key == "None":
        query = sql.SQL(
            """
            INSERT INTO {table} ({col_names}) VALUES ({values})
            """
        ).format(
            table=sql.Identifier(table),
            col_names=col_names,
            values=values,
        )
    elif "date_added" in columns_and_dtypes:
        query = sql.SQL(
            """
            INSERT INTO {table} ({col_names}) VALUES ({values})
            ON CONFLICT {prim_key} DO UPDATE
            SET 'date_added' = {date}
            """
        ).format(
            table=sql.Identifier(table),
            col_names=col_names,
            values=values,
            prim_key=sql.SQL(prim_key),
            date=sql.Literal(datetime.today()),
        )
    else:
        query = sql.SQL(
            """
            INSERT INTO {table} ({col_names}) VALUES ({values})
            ON CONFLICT {prim_key} DO NOTHING
            """
        ).format(
            table=sql.Identifier(table),
            col_names=col_names,
            values=values,
            prim_key=sql.SQL(prim_key),
            date=sql.Literal(datetime.today()),
        )

    return query


def get_pg_connecter(db_name: str) -> pg.extensions.connection:
    try:
        con = pg.connect(
            f"dbname={db_name} user='{user}' host='{host}' password='{password}'"
        )
        con.autocommit = True
        print("Database Exists.")
        return con

    except pg.OperationalError as Error:
        con = pg.connect(f"user='{user}' host='{host}' password='{password}'")
        con.autocommit = True
        cur = con.cursor()
        cur.execute(
            sql.SQL("CREATE DATABASE {db_name};").format(sql.Identifier(db_name))
        )
        con.close()
        print("Database Created.")
        return pg.connect(
            f"dbname='{db_name}' user='{user}' host='{host}' password='{password}'"
        )


def get_columns_and_dtypes(con, table: str) -> dict[str, str]:
    cur = con.cursor()
    command = sql.SQL(
        """
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = {table} 
        order by ordinal_position
        """
    ).format(table=sql.Literal(table))
    try:
        cur.execute(command)
        return cur.fetchall()
    except pg.OperationalError as Error:
        raise Error


def check_tables_exists(con, table: str):
    cur = con.cursor()
    command = f"Select * from {table} limit 1"
    try:
        cur.execute(command)
        if isinstance(cur.fetchall(), list):
            print("Table Exists.")
    except pg.OperationalError as Error:
        raise Error
