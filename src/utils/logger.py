import logging
import sys
from datetime import datetime

def setup_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)  # ⭐ CAMBIAR A DEBUG
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)  # ⭐ CAMBIAR A DEBUG
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    handler.setFormatter(formatter)
    
    if not logger.handlers:
        logger.addHandler(handler)
    
    return logger

