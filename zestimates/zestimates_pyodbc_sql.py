import pyodbc
import urllib.parse as urllib


class DataDeliverySQLServer:

    def __init__(self, target_server, target_db, schema_name=None, table_name=None):
        self._target_server = target_server
        self._schema_name = schema_name
        self._target_db = target_db
        self._table_name = table_name
        # Configure the Connection
        self.connect = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + self._target_server + ';DATABASE=' + self._target_db + ';Trusted_Connection=yes;'
        self.trusted_conn = urllib.quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + self._target_server + ';DATABASE=' + self._target_db + ';Trusted_Connection=yes;')
        self.conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(self.trusted_conn)

    # function to get value of _target_server
    def get_target_server(self):
        return self._target_server

    target_server = property(get_target_server)

    # function to get value of _target_db
    def get_target_db(self):
        return self._target_db

    target_db = property(get_target_db)

    # function to get value of _schema_name
    def get_schema_name(self):
        return self._schema_name

    schema_name = property(get_schema_name)

    # function to get value of _table_name
    def get_table_name(self):
        return self._table_name

    table_name = property(get_table_name)

    # function to get value of trusted_conn
    def get_trusted_conn(self):
        return self.trusted_conn

    # function to get value of connect
    def get_connect(self):
        return self.connect

    # function to get value of conn_str
    def get_conn_str(self):
        return self.conn_str

    def connect_to_sql_db(self):
        return pyodbc.connect(self.connect)

    def cursor_to_sql_db(self):
        return self.connect_to_sql_db().cursor()

    def execute_to_sql_db(self, query):
        sql_results = self.cursor_to_sql_db().execute(query).fetchall()
        print(sql_results)
