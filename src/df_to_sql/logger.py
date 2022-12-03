import sys
import os
import logging
from logging import handlers

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)s - %(message)s')

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
for handler in list(logger.handlers):
    logger.removeHandler(handler)

streamHandler = logging.StreamHandler(sys.stdout)
streamHandler.setLevel(logging.DEBUG)
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)

rotatingFileHandler = handlers.RotatingFileHandler(
    filename='import.log',
    mode='a',
    maxBytes=5*1024*1024,
    backupCount=5,
    encoding=None,
    delay=0
)

rotatingFileHandler.setLevel(logging.DEBUG)
rotatingFileHandler.setFormatter(formatter)
logger.addHandler(rotatingFileHandler)