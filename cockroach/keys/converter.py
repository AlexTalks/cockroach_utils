#!/usr/bin/env python3
"""
Utility for converting CockroachDB Keys.

NB: This code is EXTREMELY untested and is likely incorrect in many ways.
As such, it should NOT be used as a reference of any kind.
"""
from bidict import bidict, MutableBidict, ON_DUP_DROP_OLD
import sys
from typing import Dict, Generator, List, Text

from . import Key, KeyBuffer
from cockroach.keys import constants, encoding

SEPARATOR = "/"
TODO_KEY = Key(0x70D0.to_bytes(2, byteorder=sys.byteorder))
TODO_TABLE_KEY = Key(0x7AB1EE.to_bytes(3, byteorder=sys.byteorder))
UNSUPPORTED_KEY = Key(0xDEADBEEF.to_bytes(4, byteorder=sys.byteorder))

KEY_DICT: Dict[Text, Key] = {
    "Min": constants.KEY_MIN,
    "Max": constants.KEY_MAX,
    "Local": constants.PrefixByte.LOCAL,

    # TODO(sarkesian): support Local-specific keys
    "Store": UNSUPPORTED_KEY,
    "RangeID": UNSUPPORTED_KEY,
    "Range": UNSUPPORTED_KEY,
    "Lock": UNSUPPORTED_KEY,

    "Meta1": constants.PrefixByte.META1,
    "Meta2": constants.PrefixByte.META2,

    "System": constants.PrefixByte.SYSTEM,

    # TODO(sarkesian): support System-specific keys
    "NodeLiveness": UNSUPPORTED_KEY,
    "NodeLivenessMax": UNSUPPORTED_KEY,
    "StatusNode": UNSUPPORTED_KEY,
    "tsd": UNSUPPORTED_KEY,
    "SystemSpanConfigKeys": UNSUPPORTED_KEY,

    # TODO(sarkesian): support NamespaceTable keys
    "NamespaceTable": UNSUPPORTED_KEY,

    # NB: Only System tenant table keys currently supported.
    "Table": TODO_TABLE_KEY,

    # TODO(sarkesian): support Tenant keys
    "Tenant": UNSUPPORTED_KEY,
}

BI_KEY_DICT: MutableBidict[Text, Key] = bidict()
BI_KEY_DICT.putall(KEY_DICT, on_dup=ON_DUP_DROP_OLD)


def key_to_str(key: Key) -> Text:
    parts: List[Text] = ['']

    for byte_part in sep_bytes(key):
        if byte_part in BI_KEY_DICT.inverse:
            parts.append(BI_KEY_DICT.inverse[byte_part])
        else:
            raise Exception("unknown byte")

    return SEPARATOR.join(parts)


def sep_bytes(key: Key) -> Generator[Key, None, None]:
    if not key:
        yield b''
        return

    last_byte = b''
    for byte in key:
        single_byte = byte.to_bytes(1, byteorder=sys.byteorder)
        if single_byte == b'\xFF':
            last_byte += single_byte
            continue
        yield last_byte + single_byte
        if last_byte:
            last_byte = b''

    yield last_byte


def str_to_key(str_key: Text) -> Key:
    buf = KeyBuffer()

    for key_bytes in sep_key_parts(str_key):
        buf.extend(key_bytes)

    return Key(buf)


def sep_key_parts(str_key: Text) -> Generator[Key, None, None]:
    parts = str_key.split(SEPARATOR)
    unyielded: List[Text] = []

    def table_in_progress() -> bool:
        return bool(unyielded and unyielded[0] == "Table")

    for key_part in parts:
        if not key_part:
            continue

        mapped_bytes = KEY_DICT.get(key_part, TODO_KEY)
        if mapped_bytes not in {TODO_KEY, TODO_TABLE_KEY, UNSUPPORTED_KEY}:
            yield mapped_bytes
        elif mapped_bytes == TODO_TABLE_KEY:
            unyielded.append(key_part)
            continue
        elif mapped_bytes == TODO_KEY:
            if table_in_progress():
                if len(unyielded) < 2:
                    unyielded.append(key_part)
                else:
                    yield parse_table(unyielded[1], key_part)
                    unyielded = unyielded[2:]
            else:
                yield encoding.encode_string(Key(), key_part.strip('"'))
        else:
            raise Exception("unhandled key type")

    if table_in_progress():
        if len(unyielded) == 2:
            yield parse_table(unyielded[1], "")
            unyielded = unyielded[2:]
        else:
            raise Exception("Incorrectly formed table key")

    if unyielded:
        raise Exception("malformed key")

    return


def parse_table(table_id_str: Text, index_id_str: Text) -> Key:
    buf = KeyBuffer()
    table_id = int(table_id_str)
    buf = encoding.encode_varint(buf, table_id)

    if len(index_id_str):
        index_id = int(index_id_str)
        buf = encoding.encode_varint(buf, index_id)

    return Key(buf)
