import logging
from datetime import datetime
from multiprocessing import Queue, Process

from text_extract_helper import get_data, extract_data, dump_text, dump_pdf

WORKERS_NUM = 8

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class DocumentWorker(Process):
    def __init__(self, number, queue):
        super().__init__()
        self.number = number
        self.queue = queue

    def run(self):
        logger.debug(f'Start worker:{self.number} on:{datetime.now()}')
        try:
            for image in get_data(self.queue):
                # extract
                text, pdf = extract_data(image)
                # dump text
                dump_text(False, text)
                # dump pdf
                dump_pdf(pdf)
        finally:
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
