import os

# env variables for cloud and default value points to dev env
HOST_NAME = os.getenv('db_host', 'localhost')
PORT = os.getenv('db_port', 5432)
USER_NAME = os.getenv('db_user', 'postgres')
PASSWORD = os.getenv('db_password', 'postgres')
DB_NAME = os.getenv('db_name', 'oa_model')
DB_VIEW = os.getenv("red_shift_view", 'oa_data')


# RedShift Config
query = "select * from {}".format(DB_VIEW)

# AWS CONFIG
SECRET_CODE = ''
ACCESS_KEY = ''



