"""
This file provides internal error codes for any error that raises DynamoException
"""

__author__ = 'samuels'

SAMEFILE = 0x100  # attempt to move file to itself from different mount points (devices)
NO_TARGET = 0x101  # Target not specified
MAX_DIR_SIZE = 0x102  # Directory entry reached max size limit
ZERO_SIZE = 0x103  # Empty file. Can happen due to raise, when process trying to read file, which is still empty
HASHERR = 0x104  # Data pattern validation after writing on disk failed
