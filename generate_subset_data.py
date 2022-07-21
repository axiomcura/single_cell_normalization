import argparse
import sqlite3
from pathlib import Path

import pandas as pd


def _clean_raw_table_output(table_records: list) -> list[str]:
    """Cleans sqlite3 column name output

    Parameters
    ----------
    table_records : list
        list of sqlite3 column names

    Returns
    -------
    list[str]
        cleaned column name output
    """
    cleaned_records = [record[0] for record in table_records if len(record) > 0]
    return cleaned_records


def get_table_names(sql_cursor) -> list[str]:
    """Returns a list of tables within database

    Parameters
    ----------
    sql_cursor :
        conencted database file

    Returns
    -------
    list[str]
        list of table names
    """

    # getting all table names
    sqlite_select_Query = 'SELECT name from sqlite_master where type= "table"'
    sql_cursor.execute(sqlite_select_Query)
    table_records = _clean_raw_table_output(list(sql_cursor.fetchall()))

    return table_records


def total_entries(sql_cursor) -> int:
    """Searches the number of entries each table has and returns a number that

    Returns
    -------
    int
        number of entries
    """
    table_names = get_table_names(sql_cursor=sql_cursor)

    n_entries_list = []
    for table in table_names:
        if table == "Image":
            continue

        query = "SELECT Count() FROM %s" % table
        n_rows = sql_cursor.execute(query).fetchone()[0]
        n_entries_list.append(n_rows)

    return min(n_entries_list)


def subset_plate_data(db_path: str, outname: str, n_samples=1000) -> None:
    """_summary_

    Parameters
    ----------
    db_path : _type_
        _description_
    outname : str
        Name of the new subset sqlite database
    n_samples : int, optional
        Number of entries to be obtained , by default 1000

    Returns
    -------
    None
        Generates subset sqlite file
    """

    # connect database
    print("Connecting to database")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # getting all table names
    table_names = get_table_names(sql_cursor=cursor)

    # iterating all tables and only selecting n rows
    n_entries = total_entries(cursor)
    if n_samples > n_entries:
        raise ValueError(
            "Cannot create subset larger than the number of entries of original database."
        )

    # creating subset sqlite file
    if not outname.endswith(".sqlite"):
        print("Warning: extension was not found, adding sqlite extension.")
        outname = f"{outname}.sqlite3"

    # subs-setting data
    print("Creating new sqlite subset and populate with subset data")
    subset_sql = sqlite3.connect(outname)
    for table in table_names:

        # setting queries
        entries_query = "SELECT * FROM %s LIMIT %s" % (table, n_samples)
        col_names_query = "SELECT * FROM %s" % table

        # not sub setting Image table
        if table == "Image":
            entries_query = "SELECT * FROM %s" % table

        # executing query
        col_names_exec = cursor.execute(col_names_query)
        entries_query_exec = cursor.execute(entries_query)

        # fetching results from query
        col_names = [desc[0] for desc in col_names_exec.description]
        entries = [entry for entry in entries_query_exec.fetchall()]

        # creating data frame from results
        table_df = pd.DataFrame(data=entries, columns=col_names)
        table_df.to_sql(table, subset_sql, if_exists="replace", index=False)


if __name__ == "__main__":

    # CLI Arguments
    parser = argparse.ArgumentParser(
        description="Generating a subset of data from given plate data"
    )
    parser.add_argument(
        "-i", "--input", type=str, required=True, help="Plate data file"
    )
    parser.add_argument(
        "-o", "--output", type=str, required=True, help="Name of subset sqlite file"
    )
    parser.add_argument(
        "-n", "--sample_size", type=int, default=10000, required=False, help=""
    )
    args = parser.parse_args()

    # creating subset directory
    path_obj = Path("subsets")
    path_obj.mkdir(exist_ok=True)

    subset_outname = f"{args.output}.sqlite"
    save_path = str((path_obj / subset_outname).absolute())

    # subset data
    subset_plate_data(db_path=args.input, outname=save_path, n_samples=args.sample_size)

    # checking if the file exists
    print("Checking if subset sqlite file is generated ...")
    if not Path(save_path).is_file():
        raise FileNotFoundError("Failed to create subset sqlite file")

    print(f"Processes complete! subset saved: {save_path}")
