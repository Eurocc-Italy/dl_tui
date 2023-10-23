"""
Functions and utilities to interface with the MongoDB database on the VM

!!! WIP !!!

Author: @lbabetto
"""

import logging

logger = logging.getLogger(__name__)

USER = "user"
PASSWORD = "passwd"
IP = "131.175.207.101"
PORT = "27017"

MONGODB_URI = f"mongodb://{USER}:{PASSWORD}@{IP}:{PORT}/"
