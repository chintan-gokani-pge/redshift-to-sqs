import psycopg2
from psycopg2._psycopg import connection
import numpy as np


class DBUtil(object):

    def __init__(self, connection_url):
        self._connection_url = None
        self._connection = None
        self._cursor = None

    def connect(self, dbname: str, host: str, port: int, user_name: str, password: str):
        self._connection_url = "dbname={} host={} port={} user={} password={}".format(dbname, host, port, user_name, password)
        self._connection = psycopg2.connect(self._connection_url)
        return self._exec_cursor(self._connection)

    def connect(self, connection_url: str):
        if not connection_url or isvalid_url(connection_url):
            #throw error
            return None

        self._connection_url = connection_url
        self._connection = psycopg2.connect(self._connection_url)
        return self._exec_cursor(self._connection)

    def _exec_cursor(self, connection: connection):
        self._cursor = connection.cursor()
        return self._cursor

    def close(self):
        if self._connection:
            self._cursor.close()
            self._connection.close()
            print("PostgreSQL connection is closed")


@staticmethod
def isvalid_url(url):
    return False

session = DBUtil()


# def read_data_from_view(view : str)-> np.array:
#     print('read_data_from_view')
#     cursor = session.connect(con.DB_NAME, con.HOST_NAME, con.PORT, con.USER_NAME, con.PASSWORD)
#     # Execute Query
#     query = "select * from {}".format(view)
#     cursor.execute(query)
#     result = cursor.fetchall()
#     print(result)
#     for row in result:
#         print("Id = ", row[0], )
#     # Using numPy
#     cursor.execute(query)
#     data = np.array(cursor.fetchall())
#     session.close()
#     return data



