#!/usr/bin/env python3

from typing import List

from cockroach.keys.keys import make_key


class PrefixByte:
    LOCAL = b'\x01'
    META1 = b'\x02'
    META2 = b'\x03'
    SYSTEM = b'\x04'
    SYSTEM_MAX = b'\x05'
    TENANT = b'\xfe'


KEY_MIN = b''
KEY_MAX = b'\xff\xff'

META1_KEY_MIN = make_key(PrefixByte.META1)
META1_KEY_MAX = make_key(PrefixByte.META1, KEY_MAX)
META2_KEY_MIN = make_key(PrefixByte.META2)
META2_KEY_MAX = make_key(PrefixByte.META2, KEY_MAX)

SYSTEM_KEY_MIN = make_key(PrefixByte.SYSTEM)
SYSTEM_KEY_MAX = make_key(PrefixByte.SYSTEM_MAX)

# NOTE: currently all local keys are skipped.
# TODO(sarkesian): Add local keys, or convert into bindings