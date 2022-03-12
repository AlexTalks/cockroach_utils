#!/usr/bin/env python3

import base64
from typing import Text

from . import Key
from cockroach.keys.converter import str_to_key, key_to_str


class RoachKey:
    def __init__(self, key: Key) -> None:
        self.key = key

    @staticmethod
    def from_string(key_str: Text) -> 'RoachKey':
        return RoachKey(str_to_key(key_str))

    def to_hex(self) -> str:
        return self.key.hex()

    def to_base64(self) -> bytes:
        return base64.b64encode(self.key)

    def __str__(self) -> Text:
        raise NotImplementedError("not yet working")
        return key_to_str(self.key)
