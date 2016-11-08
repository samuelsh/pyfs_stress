"""
This file provides internal test error codes for any error that raises DynamoException
"""

__author__ = 'samuels'

SAMEFILE = 0x10000  # attempt to move file to itself from different mount points (devices)
NO_TARGET = 0x10001  # Target not specified
MAX_DIR_SIZE = 0x10002  # Directory entry reached max size limit
