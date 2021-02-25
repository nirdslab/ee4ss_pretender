#!/usr/bin/env python3

import asyncio
import sys
import logging
from threading import Thread
from asyncio.streams import StreamReader, StreamWriter

from datetime import datetime
from typing import Dict

logger = logging.getLogger(__file__)
logger.level = logging.DEBUG
logger.addHandler(logging.StreamHandler(sys.stdout))


class Stream:
    def __init__(self, l: str, t: str, fr: float, val: list, paused: bool = True):
        self.label = l
        self.type = t
        self.freq = fr or 0.1
        self.value = val
        self.last_send = 0.0
        self.paused = paused

    async def read(self):
        while True:
            t = now()
            if not self.paused and (t - self.last_send) > 1 / self.freq:
                self.last_send = t
                yield f"{self.label} {t} {' '.join(map(str, self.value))}"
            await asyncio.sleep(1 / self.freq)

    def pause(self):
        self.paused = True

    def resume(self):
        self.paused = False


class StreamHolder:

    def __init__(self) -> None:
        self.streams: Dict[str, Stream] = {
            "acc": Stream("E4_Acc", "acc", 32, [100, 200, 300]),
            "bvp": Stream("E4_Bvp", "bvp", 64, [400]),
            "ibi": Stream("E4_Ibi", "ibi", 0, [500]),
            "gsr": Stream("E4_Gsr", "gsr", 4,  [600]),
            "tmp": Stream("E4_Temperature", "tmp", 4, [700]),
            "hr": Stream("E4_Hr", "hr", 0, [800]),
            "bat": Stream("E4_Bat", "bat", 0, [1.0]),
            "tag": Stream("E4_Tag", "tag", 0, [1000])
        }
        self.is_paused = False

    def resume_all(self):
        for stream_id in self.streams:
            self.streams[stream_id].resume()
        self.is_paused = False

    def pause_all(self):
        for stream_id in self.streams:
            self.streams[stream_id].pause()
        self.is_paused = True

    def paused(self) -> bool:
        return self.is_paused

    @staticmethod
    async def __task__(s: Stream, w: StreamWriter):
        async for data in s.read():
            w.write(f"{data}\r\n".encode())

    def new_streaming_loop(self, w: StreamWriter):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        tasks = []
        for stream_id in self.streams:
            tasks.append(loop.create_task(self.__task__(self.streams[stream_id], w)))
        loop.run_forever()


def now():
    '''Helper function to generate timestamps in EE4 format'''
    return datetime.now().timestamp()


def cmd_handler(msg: str) -> str:
    # If msg does not exist, or cmd empty, return empty
    if not msg:
        return ""

    msg_parts = msg.split()
    cmd, args = msg_parts[0], msg_parts[1:]

    if (cmd == "device_list"):
        return "R device_list 2 | 9ff167 Empatica_E4 | 7a3166 Empatica_E4"

    if (cmd == "device_connect"):
        return "R device_connect OK"

    if (cmd == "device_subscribe"):
        if len(args) > 2:
            return "R device_subscribe ERR too many arguments"
        sub = args[0].lower()
        if sub in STREAM_TYPES:
            return f"R {cmd} {sub} OK"

    if (cmd == "pause"):
        if len(args) > 1:
            return "R pause ERR too many arguments"
        else:
            if args[0].upper() == "ON":
                if STREAMS.paused():
                    return "R pause ERR already paused"
                STREAMS.pause_all()
                return "R pause ON"
            elif args[0].upper() == "OFF":
                if not STREAMS.paused():
                    return "R pause ERR not paused"
                STREAMS.resume_all()
                return "R pause OFF"
            else:
                return "R pause ERR wrong argument"

    return ""


async def ee4_srv(r: StreamReader, w: StreamWriter):
    thread = Thread(target=STREAMS.new_streaming_loop, args=(w,))
    thread.start()
    while True:
        try:
            msg_raw = await r.readline()
            msg = msg_raw.decode().strip()
            logger.info(f"< {msg}")
            out = cmd_handler(msg)
            logger.info(f"> {out}")
            w.write(f"{out}\r\n".encode())
            await w.drain()
        except UnicodeDecodeError:
            logger.warn("invalid char found")
            break
        except AttributeError:
            logger.warn("attribute error found")
            break


async def main():
    srv = await asyncio.start_server(ee4_srv, '127.0.0.1', 28000)
    host, port = srv.sockets[0].getsockname()
    logger.info(f"Streaming server started on {host}:{port}!")
    try:
        async with srv:
            await srv.serve_forever()
    except KeyboardInterrupt:
        exit(0)

# For testing...
if (__name__ == "__main__"):
    # Global to handle streaming
    STREAMS = StreamHolder()
    STREAM_TYPES = ["acc", "bvp", "gsr", "ibi", "tmp", "bat", "tag", "hr"]
    # In try-except to suppress irrelevant errors
    try:
        asyncio.run(main(), debug=True)
    except KeyboardInterrupt:
        exit(0)
