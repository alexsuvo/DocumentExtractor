import base64
import io
import os
import random
import shutil
from datetime import datetime
from multiprocessing import Queue, Lock
from unittest import TestCase

import pdf2jpg
from PIL import Image
from PyPDF2 import PdfFileMerger
from pdf2jpg.pdf2jpg import convert_pdf2jpg

from text_extract_helper import extract_data, extract_multipage_data, get_data


class HelperTest(TestCase):

    def setUp(self) -> None:
        self.clean_up()

    def tearDown(self) -> None:
        self.clean_up()

    def clean_up(self):
        # clean input
        output_root = os.path.join(os.getcwd(), 'Output')
        for f in os.listdir(output_root):
            path = os.path.join(output_root, f)
            if os.path.isdir(path) and f.startswith('test_'):
                shutil.rmtree(path)
            if os.path.isfile(path) and f.startswith('test_'):
                os.remove(path)
        # clean output
        input_root = os.path.join(os.getcwd(), 'Input')
        for f in os.listdir(input_root):
            path = os.path.join(input_root, f)
            if os.path.isdir(path) and f.startswith('test_'):
                shutil.rmtree(path)
            if os.path.isfile(path) and f.startswith('test_'):
                os.remove(path)

    def test_get_data(self):
        counter = 0
        list_id = []
        lock = Lock()
        for key, blob in get_data(lock, list_id):
            if counter < 1:
                counter = counter + 1
                # assert
                self.assertTrue(len(key) > 0)
                self.assertTrue(blob is not None)

    def test_extract_data(self):
        counter = 0
        list_id = []
        lock = Lock()
        for key, blob in get_data(lock, list_id):
            if counter < 1:
                counter = counter + 1
                print(f'Got file {key}: {datetime.now()}')
                text, pdf = extract_multipage_data(key, blob)
                # assert
                self.assertTrue(len(text) > 0)
                self.assertTrue(pdf is not None)
                print(f'Done with file {key}: {datetime.now()}')
        # done
        print('done')
