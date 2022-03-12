#!/usr/bin/env python3

Key = bytes
KeyBuffer = bytearray


class RoachKey:
    def __init__(self, key: Key) -> None:
        self.key = key


def make_key(*args: Key) -> Key:
    return Key(KeyBuffer().join(args))
