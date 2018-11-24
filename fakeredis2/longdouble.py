"""Basic long double arithmetic using the system C library"""

import ctypes
from ctypes import CDLL, POINTER, c_longdouble, c_char_p
from ctypes.util import find_library

libc_library = find_library('c') or find_library('msvcrt') or find_library('System')

if not libc_library:
    raise ImportError('fakeredis: unable to find libc or equivalent')

libc = CDLL(libc_library)
libc.strtold.restype = c_longdouble
libc.strtold.argtypes = [c_char_p, POINTER(c_char_p)]
libc.fmal.restype = 

class LongDouble(object):
    def __init__(
