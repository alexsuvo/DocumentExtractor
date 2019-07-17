import base64
import io
import os
import shutil
import tempfile

import cx_Oracle
import requests
from PIL import Image
from PyPDF2 import PdfFileWriter, PdfFileReader
from pdf2jpg.pdf2jpg import convert_pdf2jpg
from pytesseract import pytesseract

ORCL_CS = 'bps_scan_doc/anit1234@//sdbahpocm01-db.agriculture.gov.ie:1532/NPSS.agriculture.gov.ie'
ORCL_SQL_SELECT = 'select SDS_SCANNED_DOCUMENT_ID, SDI_IMAGE from BPS_SCAN_DOC.TDCR_SCANNED_DOCUMENT_IMAGES'
ORCL_SQL_INSERT = 'INSERT INTO topic_modeler_dataraw(pdf) VALUES(:val);'

DUMP_URL = 'http://localhost:8000/rest_api/data_raw/'


def contains_id(lock, list_id, id):
    try:
        lock.acquire()
        if id in list_id:
            return True
        else:
            list_id.append(id)
            return False
    finally:
        lock.release()


def get_data(lock, list_id):
    oracle_connection = cx_Oracle.connect(ORCL_CS)
    oracle_cursor = oracle_connection.cursor()
    oracle_cursor.execute(ORCL_SQL_SELECT)
    try:
        # iterate over cursor result
        for result in oracle_cursor:
            # id of the file
            file_id = result[0]
            # processed or about to be
            if not contains_id(lock, list_id, file_id):
                # content of the file
                file_blob = result[1]
                # make image from bytes
                yield file_id, file_blob.read()
    finally:
        # close cursor and connection
        oracle_cursor.close()
        oracle_connection.close()


def extract_data(image):
    # extract text
    text = pytesseract.image_to_string(image)
    # convert to PDF
    pdf = pytesseract.image_to_pdf_or_hocr(image)
    # done
    return text, pdf


def extract_multipage_data(key, image):
    # file path
    input_file_path = os.path.join(tempfile.gettempdir(), f'{key}.blob')
    # path for output
    output_file_path = os.path.join(tempfile.gettempdir(), f'{key}')
    # create dir
    os.mkdir(output_file_path)
    try:
        # dump pdf file
        with open(input_file_path, "wb") as file:
            file.write(image)
        # pdf to images
        result = convert_pdf2jpg(input_file_path, output_file_path, dpi=300, pages="ALL")
        # process images
        all_text = []
        all_pdf = []
        for index, value in enumerate(result[0]['output_jpgfiles']):
            # extract
            text, pdf = extract_data(Image.open(value))
            # store text
            all_text.append(text)
            # dump pdf
            path_pdf = os.path.join(tempfile.gettempdir(), f'{key}', f'{key}_{index}.pdf')
            with open(path_pdf, 'wb') as fout:
                fout.write(pdf)
            # store filepath
            all_pdf.append(path_pdf)
            # remove image
            os.remove(value)
        # combine text
        combine_text = ' '.join([x for x in all_text]).encode('utf-8')
        # combine pdf
        pdf_writer = PdfFileWriter()
        for x in all_pdf:
            pdf_reader = PdfFileReader(x)
            for page in range(pdf_reader.getNumPages()):
                pdf_writer.addPage(pdf_reader.getPage(page))
            # remove old pdf
            os.remove(x)
        # dump all pdf file
        path_pdf = os.path.join(tempfile.gettempdir(), f'{key}', f'{key}_all.pdf')
        with open(path_pdf, 'wb') as fout:
            pdf_writer.write(fout)
        # read and encode
        with open(path_pdf, 'rb') as fin:
            data = base64.b64encode(fin.read())
        # remove file
        os.remove(path_pdf)
        # done
        return combine_text, data
    finally:
        # delete file
        if os.path.exists(input_file_path):
            os.remove(input_file_path)
        # delete images
        if os.path.exists(output_file_path):
            shutil.rmtree(output_file_path)


def dump_text(db, text):
    # rest call
    tmp = {'text': text}
    requests.post(DUMP_URL, data=tmp)


def dump_pdf(pdf):
    oracle_connection = cx_Oracle.connect(ORCL_CS)
    oracle_cursor = oracle_connection.cursor()
    try:
        if pdf:
            mem_file = io.BytesIO(pdf)
            oracle_cursor.execute(ORCL_SQL_INSERT, mem_file.getvalue())
            oracle_connection.commit()
    finally:
        # close cursor and connection
        oracle_cursor.close()
        oracle_connection.close()
