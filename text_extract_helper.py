import io
import os
import shutil
from datetime import datetime

import cx_Oracle
import numpy as np
import psycopg2
import requests
from PIL import Image
from pdf2jpg import pdf2jpg
from pytesseract import pytesseract

PGA_CS = 'postgresql://dafm:dafm123~@localhost:5432/dafm_topic_modeling'
PGA_SQL = 'INSERT INTO topic_modeler_dataraw(text, created_date) VALUES(%s, %s);'

ORCL_CS = 'scott/tiger@host:1521/orcl'
ORCL_SQL_SELECT = 'select id, blob from'
ORCL_SQL_INSERT = 'INSERT INTO topic_modeler_dataraw(text) VALUES(%s);'

DUMP_URL = 'http://localhost:8000/rest_api/data_raw/'


def get_data(queue):
    oracle_connection = cx_Oracle.connect(ORCL_CS)
    oracle_cursor = oracle_connection.cursor()
    oracle_cursor.execute(ORCL_SQL_SELECT)
    try:
        # iterate over cursor result
        for result in oracle_cursor:
            # id of the file
            file_id = result[0]
            # processed or about to be
            if file_id not in queue:
                # mark as such
                queue.put(file_id)
                # content of the file
                file_blob = result[1]
                # make image from bytes
                yield file_id, file_blob.read()
    finally:
        # close cursor and connection
        oracle_cursor.close()
        oracle_connection.close()


def extract_data(image):
    # greyscale
    image = image.convert('LA')
    # extract text
    text = pytesseract.image_to_string(image)
    # convert to PDF
    pdf = pytesseract.image_to_pdf_or_hocr(image)
    # done
    return text, pdf


def extract_multipage_data(key, image):
    # file path
    inut_file_path = os.path.join(os.getcwd(), 'Input', key)
    # path for output
    output_file_path = os.path.join(os.getcwd(), 'Output', key)
    try:
        # dump file
        with open(inut_file_path, 'wb') as file:
            file.write(image)
        # create output directory
        os.mkdir(output_file_path)
        # to convert all pages
        result = pdf2jpg.convert_pdf2jpg(inut_file_path, output_file_path, dpi=300, pages="ALL")
        # combine all images into one
        imgs = [Image.open(i) for i in result[0]['output_jpgfiles']]
        min_shape = sorted([(np.sum(i.size), i.size) for i in imgs])[0][1]
        imgs_comb = np.vstack((np.asarray(i.resize(min_shape)) for i in imgs))
        # done
        return extract_data(Image.fromarray(imgs_comb))
    finally:
        # delete file
        if os.path.exists(inut_file_path):
            os.remove(inut_file_path)
        # delete images
        if os.path.exists(output_file_path):
            shutil.rmtree(output_file_path)


def dump_text(db, text):
    postgres_connection = psycopg2.connect(PGA_CS)
    postgres_cursor = postgres_connection.cursor()
    try:
        # where to go
        if db:
            # store in postgres
            postgres_cursor.execute(PGA_SQL, (text, datetime.now()))
            postgres_connection.commit()
        else:
            # rest call
            tmp = {'text': text}
            requests.post(DUMP_URL, data=tmp)
    finally:
        postgres_cursor.close()
        postgres_connection.close()


def dump_pdf(pdf):
    oracle_connection = cx_Oracle.connect(ORCL_CS)
    oracle_cursor = oracle_connection.cursor()
    oracle_cursor.execute(ORCL_SQL_INSERT)
    try:
        # iterate over cursor result
        if pdf:
            pass
    finally:
        # close cursor and connection
        oracle_cursor.close()
        oracle_connection.close()
