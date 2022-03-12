#!/usr/bin/env python3

Key = bytes
KeyBuffer = bytearray


def make_key(*args: Key) -> Key:
    return Key(KeyBuffer().join(args))
