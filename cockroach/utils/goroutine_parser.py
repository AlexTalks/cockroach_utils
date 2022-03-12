#! /usr/bin/env python3

import re
from typing import Dict, List, Mapping, TextIO

rx_goroutine = re.compile(
    r'(?:goroutine )([\d]+) \[(.*?)(?:, )?([\d]+ minutes)?\]'
)
rx_codeline = re.compile(
    r'(?:[\s]*)(.*\.(go|s):[\d]+).*'
)


class GoroutineData:
    def __init__(self, r_id: int, state: str, wait_mins: str) -> None:
        self.r_id = r_id
        self.state = state
        self.wait_mins = wait_mins
        self.trace: List[str] = list()

    def __repr__(self) -> str:
        return ''.join(self.trace)


goroutines_by_id: Dict[int, GoroutineData] = dict()
goroutines_by_state: Dict[str, List[int]] = dict()
goroutines_by_wait: Dict[str, List[int]] = dict()
goroutines_by_line: Dict[str, List[int]] = dict()


def store_goroutine(gr: GoroutineData) -> None:
    goroutines_by_id[gr.r_id] = gr
    rx_match = rx_codeline.match(gr.trace[2])
    assert rx_match
    line, _ = rx_match.groups()
    if gr.state not in goroutines_by_state:
        goroutines_by_state[gr.state] = []
    goroutines_by_state[gr.state].append(gr.r_id)
    if gr.wait_mins not in goroutines_by_wait:
        goroutines_by_wait[gr.wait_mins] = []
    goroutines_by_wait[gr.wait_mins].append(gr.r_id)
    if line not in goroutines_by_line:
        goroutines_by_line[line] = []
    goroutines_by_line[line].append(gr.r_id)


def parse_goroutines(in_file: TextIO) -> None:
    curr_goroutine = None
    i = 0
    for line in in_file:
        i += 1
        try:
            if len(line.strip()) == 0:
                if curr_goroutine is not None:
                    store_goroutine(curr_goroutine)
                curr_goroutine = None
                continue

            if curr_goroutine is None:
                m = rx_goroutine.match(line)
                if not m:
                    raise Exception(
                        'looking for new goroutine '
                        'but got bad line:\n\t{}'.format(line)
                    )

                gr_id, state, wait_mins = m.groups()
                if wait_mins is None:
                    wait_mins = 'none'

                curr_goroutine = GoroutineData(int(gr_id), state, wait_mins)

            curr_goroutine.trace.append(line)
        except Exception as e:
            raise Exception('error on line {0}: {1}'.format(i, e))

    if curr_goroutine is not None:
        try:
            store_goroutine(curr_goroutine)
        except Exception as e:
            raise Exception('error on line {0}: {1}'.format(i, e))


def count_by(grouping: Mapping[str, List[int]]) -> None:
    for group_by, goroutines in grouping.items():
        print('{}: {}'.format(group_by, len(goroutines)))
