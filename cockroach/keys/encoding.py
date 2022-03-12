#!/usr/bin/env python3

from typing import Union

from . import Key, KeyBuffer

INT_MAX_WIDTH = 8
INT_MIN = 0x80                                  # 128
INT_MAX = 0xfd                                  # 253
INT_ZERO = INT_MIN + INT_MAX_WIDTH              # 136
INT_SMALL = INT_MAX - INT_ZERO - INT_MAX_WIDTH  # 109

BYTES_MARKER = 0x12
BYTES_ESCAPE = 0x0001

# TODO(sarkesian): support decoding, other types


def encode_string(key: Union[Key, KeyBuffer], val: str) -> KeyBuffer:
    return encode_bytes(key, bytearray(val, "utf-8"))


def encode_bytes(key: Union[Key, KeyBuffer], val: bytes) -> KeyBuffer:
    buf = key if type(key) == KeyBuffer else KeyBuffer(key)
    buf.extend(BYTES_MARKER.to_bytes(1, byteorder="big"))
    buf.extend(val)
    buf.extend(BYTES_ESCAPE.to_bytes(2, byteorder="big"))
    return buf


def encode_varint(key: Union[Key, KeyBuffer], val: int) -> KeyBuffer:
    buf = key if type(key) == KeyBuffer else KeyBuffer(key)
    if val <= INT_SMALL:
        buf.append(INT_ZERO + val)
    elif val <= 0xff:
        buf.append(INT_MAX-7)
        buf.extend(val.to_bytes(1, byteorder="big"))
    elif val <= 0xffff:
        buf.append(INT_MAX-6)
        buf.extend(val.to_bytes(2, byteorder="big"))
    elif val <= 0xffffff:
        buf.append(INT_MAX-5)
        buf.extend(val.to_bytes(3, byteorder="big"))
    elif val <= 0xffffffff:
        buf.append(INT_MAX-4)
        buf.extend(val.to_bytes(4, byteorder="big"))
    elif val <= 0xffffffffff:
        buf.append(INT_MAX-3)
        buf.extend(val.to_bytes(5, byteorder="big"))
    elif val <= 0xffffffffffff:
        buf.append(INT_MAX-2)
        buf.extend(val.to_bytes(6, byteorder="big"))
    elif val <= 0xffffffffffffff:
        buf.append(INT_MAX-1)
        buf.extend(val.to_bytes(7, byteorder="big"))
    else:
        buf.append(INT_MAX)
        buf.extend(val.to_bytes(8, byteorder="big"))

    return buf
