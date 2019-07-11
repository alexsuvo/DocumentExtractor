import base64
import os
from unittest import TestCase

from text_extract_helper import extract_data


class HelperTest(TestCase):

    def test_extract_data(self):
        # path
        path = os.path.join(os.getcwd(), 'Data', 'test_1.PNG')
        # read file
        with open(path, "rb") as file:
            data = file.read()
        # extract
        text, pdf = extract_data(data)
        # assert
        self.assertTrue(len(text) > 0)
        self.assertTrue(pdf is not None)
        # dump pdf
        # path
        path_pdf = os.path.join(os.getcwd(), 'Data', 'test_1.pdf')
        # dump pdf file
        with open(path_pdf, "wb") as file:
            file.write(pdf)
        path_txt = os.path.join(os.getcwd(), 'Data', 'test_1.txt')
        # dump txt file
        with open(path_txt, "w+") as file:
            file.write(text)
        # done
        print('done')
