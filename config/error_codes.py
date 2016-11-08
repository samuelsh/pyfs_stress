"""
This file provides internal error codes for any error that raises DynamoException
"""

__author__ = 'samuels'

SAMEFILE = 0x100  # attempt to move file to itself from different mount points (devices)
NO_TARGET = 0x101  # Target not specified
MAX_DIR_SIZE = 0x102  # Directory entry reached max size limit
