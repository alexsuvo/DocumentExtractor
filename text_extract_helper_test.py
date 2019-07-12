import base64
import io
import os
import random
from unittest import TestCase

from PIL import Image

from text_extract_helper import extract_data, extract_multipage_data


class HelperTest(TestCase):

    def test_extract_data(self):
        # path
        path = os.path.join(os.getcwd(), 'Input', 'test_1.PNG')
        # read file
        with open(path, "rb") as file:
            data = file.read()
        # extract
        text, pdf = extract_data(Image.open(io.BytesIO(data)))
        # assert
        self.assertTrue(len(text) > 0)
        self.assertTrue(pdf is not None)
        # dump pdf
        path_pdf = os.path.join(os.getcwd(), 'Output', 'test_1.pdf')
        # dump pdf file
        with open(path_pdf, "wb") as file:
            file.write(pdf)
        # dump txt file
        path_txt = os.path.join(os.getcwd(), 'Output', 'test_1.txt')
        with open(path_txt, "w+") as file:
            file.write(text)




    def test_extract_data_pdf(self):
        # path
        path = os.path.join(os.getcwd(), 'Input', 'test_2.pdf')
        # read file
        with open(path, "rb") as file:
            data = file.read()
        # extract
        text, pdf = extract_multipage_data('multipage_pdf', data)
        # assert
        self.assertTrue(len(text) > 0)
        self.assertTrue(pdf is not None)
        # dump pdf
        # path
        path_pdf = os.path.join(os.getcwd(), 'Output', 'multipage_pdf_updated.pdf')
        # dump pdf file
        with open(path_pdf, "wb") as file:
            file.write(pdf)
        path_txt = os.path.join(os.getcwd(), 'Output', 'multipage_pdf_updated.txt')
        # dump txt file
        with open(path_txt, "w+") as file:
            file.write(text)
        # done
        print('done')