import logging
from datetime import datetime as dt
import os
import csv
import io
# from utilities import set_frame

class CsvFormatter(logging.Formatter):
    def __init__(self):
        super().__init__()
        self.output = io.StringIO()
        self.writer = csv.writer(self.output, quoting=csv.QUOTE_ALL)

    def format(self, record):
        timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        
        self.writer.writerow([timestamp, record.levelname, record.msg])
        data = self.output.getvalue()
        self.output.truncate(0)
        self.output.seek(0)
        return data.strip()

def init_logger(log_module):

    logger = logging.getLogger(log_module)
    logger.setLevel(logging.DEBUG)
    directory = os.path.abspath('logs')
    now = dt.now().strftime("%Y%m%d_%H%M%S")
    
    filename = os.path.join(directory, f'logs_{now}.csv')
    # try:
    #     logger.handlers[0].stream.close()
    #     logger.removeHandler(logger.handlers[0])
    # except:
    #     print("Unable to close logger stream")
    file_handler = logging.FileHandler(filename)
    if debug_mode:
        file_handler.setLevel(logging.DEBUG)
    else:
        file_handler.setLevel(logging.INFO)
    # formatter = logging.Formatter('%(asctime)s, %(levelname)s, %(message)s')
    file_handler.setFormatter(CsvFormatter())
    logger.addHandler(file_handler)
    
    # print(logger.handlers)
   #  logger.info("Initiated Logger")
    return logger
    


debug_mode = False
inputs = {}
logger = init_logger("ResourceOptimizer")

