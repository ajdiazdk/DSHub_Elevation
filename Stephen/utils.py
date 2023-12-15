import os
import sys
from getpass import getpass
from pathlib import Path

from psycopg2 import OperationalError, ProgrammingError, connect


def db_params(
    host: str = "10.11.11.10",
    user: str = "meta",
    password: str = None,
    dbname: str = "metadata",
    port: int = 6432,
):
    """
    Generate database connection parameters, which can be passed to
    `get_db_connection()` to make a database connection. By default, parameters
    are generated for the metadata database on server 10.11.11.10.

    Parameters
    ----------
    host : str, optional
        postgresql database host address, by default "10.11.11.10"
    user : str, optional
        database user name used to authenticate, by default "meta"
    password : str, optional
        database password used to authenticate, by default None
    dbname : str, optional
        the database name, by default "metadata"
    port : int, optional
        database connection port number, by default 6432

    Returns
    -------
    dict
        database connection parameters

    Examples
    --------
    Default connection parameters
    >>> meta_db_params = db_params()

    Return connection parameters for elevation database
    >>> elev_db_params = db_params(user="elevation", dbname="elevation")
    """
    if password is None:
        pwd = getpass(f"Postgres Password for {dbname}:")
    else:
        pwd = password

    params = {
        "host": host,
        "user": user,
        "password": pwd,
        "dbname": dbname,
        "port": port,
    }
    return params


def get_db_connection(db_params: dict):
    """
    Return a database connection

    Parameters
    ----------
    db_params : dict
        database connection parameters

    Returns
    -------
    psycopg2.connection
        database connection

    Examples
    --------
    Make a database connection using the default parameter dictionary returned by db_params()
    >>> meta_db_conn = get_db_connection(db_params())

    Make a connection to the elevation database on 10.11.11.10
    >>> elev_db_params = db_params(user="elevation", dbname="elevation")
    >>> elev_db_conn = get_db_connection(elev_db_params)
    """
    if db_params is None:
        raise TypeError("[*] DATABASE PARAMETERS CANNOT BE NONE")
    if isinstance(db_params, dict) == False:
        raise TypeError("[*] DATABASE PARAMETERS MUST BE A DICTIONARY")

    try:
        with connect(**db_params) as conn:
            print("[*] DB CONNECTION: SUCCESS")
            return conn
    except OperationalError as e:
        print("[*] DB CONNECTION: FAIL")
        # get details about the exception
        err_type, _, traceback = sys.exc_info()
        line_num = traceback.tb_lineno
        print(f"\n[*] ERROR: {e} on line number: {line_num}")
        print(f"[*] psycopg2 traceback: {traceback} -- type: {err_type}")
        # psycopg2 extensions.Diagnostics object attribute
        print(f"\n[*] extensions.Diagnostics: {e.diag}")
        # print the pgcode and pgerror exceptions
        print(f"[*] pgerror: {e.pgerror}")
        print(f"[*] pgcode: {e.pgcode}\n")
        return None


def execute_query(connection, query: str, return_results: bool):
    """
    Execute SQL query

    Parameters
    ----------
    connection : psycopg2.connection
        database conenction
    query : str
        SQL query
    return_results : bool
        Whether to return query results.

    Returns
    -------
    list
        If return_results is True, then return results from cursor.fetchall()

    Raises
    ------
    ConnectionError
        Database connection is None

    Examples
    --------
    >>> q = 'SELECT DISTINCT dem_path as dirpath FROM metadata.us_elevation_1m_4326_meta;'
    >>> conn = get_db_connection(db_params())
    >>> dem_dirs = execute_query(conn, query=q, return_results=True)
    """
    if connection is None:
        raise ConnectionError("[*] DATABASE CONNECTION CANNOT BE NONE")

    try:
        with connection, connection.cursor() as cursor:
            cursor.execute(query)
            if return_results == True:
                # cursor.fetchall() returns a list of tuples
                return [i[0] for i in cursor.fetchall()]
    except ProgrammingError as pe:
        print(f"[*] PG ERROR: {pe.pgerror}")
        print("[*] SQL EXECUTION: FAIL")
        return None


def list_meta_tables(connection):
    """
    Return list of elevation metadata table names from metadata database.

    Parameters
    ----------
    conn : psycopg2.connection
        Connection to metadata database

    Returns
    -------
    list
        Elevation metadata table names from metadata database

    Raises
    ------
    ConnectionError
        Database connection is None or invalid

    Examples
    --------
    >>> metadata_db_conn = get_db_connection(db_params=db_params())
    >>> table_list = list_meta_tables(connection=metadata_db_conn)
    """
    if connection is None:
        raise ConnectionError("[*] DATABASE CONNECTION CANNOT BE NONE")

    q = """
            SELECT
                tablename
            FROM
                pg_catalog.pg_tables
            WHERE
                schemaname = 'metadata'
                AND tablename LIKE '%_elevation_%_meta'
            ORDER BY 
                tablename;
            """
    table_list = execute_query(connection, query=q, return_results=True)
    return table_list


def list_db_tables(connection, schema_name: str):
    """
    Return list of elevation metadata table names from metadata database.

    Parameters
    ----------
    connection : psycopg2.connection
        Connection to metadata database
    schema_name: str
        Database schema name

    Returns
    -------
    list
        Elevation metadata table names from metadata database

    Raises
    ------
    ConnectionError
        Database connection is None or invalid

    Examples
    --------
    >>> metadata_db_conn = get_db_connection(db_params=db_params())
    >>> table_list = list_meta_tables(connection=metadata_db_conn)
    """
    if connection is None:
        raise ConnectionError("[*] DATABASE CONNECTION CANNOT BE NONE")

    q = f"""
            SELECT
                table_name
            FROM
                information_schema.tables
            WHERE
                table_schema='{schema_name}'
            ORDER BY 
                table_name ASC;
            """
    table_list = execute_query(connection, query=q, return_results=True)
    return table_list


def clean(directory: str, substring: str):
    """
    Remove files from directory which include specific substring in the filename

    Parameters
    ----------
    directory : str
        Directory to search for files within
    substring : str
        Substring to search in file names. Matched files will be removed.

    Raises
    ------
    NotADirectoryError
        Operation works on directories

    Examples
    --------
    Remove files from directories returned from metadata table
    >>> q = "SELECT DISTINCT dem_path as dirpath FROM metadata.us_elevation_1m_4326_meta;"
    >>> conn = get_db_connection(db_params())
    >>> dem_dirs = execute_query(conn, query=q, return_results=True)
    >>> for dir in dem_dirs:
    >>>     clean(directory=dir, substring="NODATA_60586972")
    """
    if Path(directory).is_dir() == False:
        raise NotADirectoryError("[*] OPERATION WORKS ON DIRECTORIES ONLY")
    if isinstance(substring, str) == False:
        raise TypeError("[*] SUBSTRING ARGUMENT MUST BE A STRING")

    oldfiles = os.listdir(directory)
    dfiles = [x for x in oldfiles if x.find(f"{substring}") > -1]
    if len(dfiles) >= 1:
        for d in dfiles:
            os.remove(os.path.join(directory, d))

    print(f"[*] CLEANUP: REMOVED {len(dfiles)} FILES IN {directory}")
