import io
import logging
from datetime import datetime
from multiprocessing import Queue, Process

import cv2
import cx_Oracle
import psycopg2
import pytesseract
import requests
from PIL import Image

WORKERS_NUM = 8
ORCL_CS = 'scott/tiger@host:1521/orcl'
ORCL_SQL = 'select id, blob from'

PGA_CS = 'postgresql://dafm:dafm123~@localhost:5432/dafm_topic_modeling'
PGA_SQL = 'INSERT INTO topic_modeler_dataraw(text, created_date) VALUES(%s, %s);'

DB_DUMP = True
DUMP_URL = 'http://localhost:8000/rest_api/data_raw/'

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class DocumentWorker(Process):
    def __init__(self, number, queue):
        super().__init__()
        self.number = number
        self.queue = queue

    def run(self):
        logger.debug(f'Start worker:{self.number} on:{datetime.now()}')
        # get oracle connection
        oracle_connection = cx_Oracle.connect(ORCL_CS)
        oracle_cursor = oracle_connection.cursor()
        # if db dump
        if DB_DUMP:
            postgres_connection = psycopg2.connect(PGA_CS)
            postgres_cursor = postgres_connection.cursor()
        else:
            postgres_connection = None
            postgres_cursor = None
        # run the sql
        oracle_cursor.execute(ORCL_SQL)
        try:
            # iterate over cursor result
            for result in oracle_cursor:
                # id of the file
                file_id = result[0]
                # processed or about to be
                if file_id not in self.queue:
                    # mark as such
                    self.queue.put(file_id)
                    # content of the file
                    file_blob = result[1]
                    # any content
                    if file_blob:
                        # make image from bytes
                        image = Image.open(io.BytesIO(file_blob))
                        # greyscale
                        image = image.convert('LA')
                        # apply filter to smooth and to reduce noise
                        image - cv2.medianBlur(image, 5)
                        # extract text
                        text = pytesseract.image_to_string(image)
                        # where to go
                        if DB_DUMP:
                            # store in postgres
                            postgres_cursor.execute(PGA_SQL, (text, datetime.now()))
                            postgres_connection.commit()
                        else:
                            tmp = {'text': text}
                            requests.post(DUMP_URL, data=tmp)
                        # done with the file
                        logger.debug(f'Worker id:{self.number} processed file:{file_id}')
        finally:
            # close cursor and connection
            oracle_cursor.close()
            oracle_connection.close()
            # if db dump
            if DB_DUMP:
                postgres_cursor.close()
                postgres_connection.close()
            # done
            logger.debug(f'Stop worker:{self.number} on:{datetime.now()}')


if __name__ == '__main__':
    # start
    logger.debug(f'Start:{datetime.now()}')
    # thread safe queue to hold files processed
    files_inuse = Queue()
    # create workers
    workers = []
    for i in range(1, WORKERS_NUM + 1):
        workers.append(DocumentWorker(i, files_inuse))
    # start workers
    for w in workers:
        w.start()
    # join workers
    for w in workers:
        w.join()
    # done
    logger.debug(f'Done:{datetime.now()}')
