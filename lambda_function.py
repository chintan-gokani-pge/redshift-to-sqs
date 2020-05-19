import json
import boto3
import time
import numpy as np
from dao.DBUtil import session, psycopg2
from util import constant as con

import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

""" read data from redshift and cache retrieved data  to SQS """

message_ids = []


def lambda_handler(event, context):
    logger.info("event detail : {}".format(event))
    start_time = time.time()
    logger.info("fn execution start_time : {}".format(event))

    logger.info("reading data from db on view : {}".format(con.DB_VIEW))
    # data = read_data_from_db_view(con.DB_VIEW)
    data = read_data_from_db_view('oa_sample')
    logger.info("fetched record size : {}".format(len(data)))

    for record in data:
        send_data_to_sqs_resource(record)

    elapsed_time = time.time() - start_time
    logger.info("fn execution elapsed_time : {}".format(elapsed_time))
    return {
        'statusCode': 200,
        'message_ids': json.dumps(message_ids)
    }


def read_data_from_db_view(view: str = None) -> np.array:
    logger.info('read_data_from_view')
    if not view:
        logger.info('database view is not present')
        return None
    try:
        cursor = session.connect(con.DB_NAME, con.HOST_NAME, con.PORT, con.USER_NAME, con.PASSWORD)
        query = "select * from {}".format(view)
        cursor.execute(query)
        return np.array(cursor.fetchall())
    except (Exception, psycopg2.Error) as error:
        logger.error("Error while fetching data from PostgresSQL", error)
    finally:
        session.close()


def send_data_to_sqs_resource(record: list, groupId: str = None):
    logger.info('send_data_to_sqs_resource')
    if record is None:
        logger.info('No record found')
        return None
    sqs = boto3.resource('sqs',
                         region_name='us-west-2',
                         aws_access_key_id=con.ACCESS_KEY,
                         aws_secret_access_key=con.SECRET_CODE
                         )
    queue = sqs.get_queue_by_name(QueueName='oa_data')
    logger.info("sqs queue name: {}".format(queue))
    response = queue.send_message(
        MessageBody=np.array_str(record),
        MessageGroupId='messageGroup1'
    )
    message_ids.append(response.get('MessageId'))
    print(response.get('MD5OfMessageBody'))


if __name__ == '__main__':
    pass
