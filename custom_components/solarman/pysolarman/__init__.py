import time
import types
import struct
import logging
import asyncio

from functools import wraps
from random import randrange
from logging import getLogger
from multiprocessing import Event

from .umodbus.functions import FUNCTION_CODES
from .umodbus.exceptions import error_code_to_exception_map
from .umodbus.client.serial.redundancy_check import get_crc
from .umodbus.client.serial import rtu
from .umodbus.client import tcp

from ..common import retry, throttle, create_task, format

_LOGGER = getLogger(__name__)

MAX_RETRIES = 4
INITIAL_BACKOFF = 3  # seconds
MAX_BACKOFF = 24     # seconds

PROTOCOL = types.SimpleNamespace()
PROTOCOL.CONTROL_CODE = types.SimpleNamespace()
PROTOCOL.CONTROL_CODE.HANDSHAKE = 0x41
PROTOCOL.CONTROL_CODE.DATA = 0x42
PROTOCOL.CONTROL_CODE.INFO = 0x43
PROTOCOL.CONTROL_CODE.REQUEST = 0x45
PROTOCOL.CONTROL_CODE.HEARTBEAT = 0x47
PROTOCOL.CONTROL_CODE.REPORT = 0x48
PROTOCOL.CONTROL_CODE_SUFFIX = bytes.fromhex("10")
PROTOCOL.CONTROL_CODES = PROTOCOL.CONTROL_CODE.__dict__.values()
PROTOCOL.FRAME_TYPE = bytes.fromhex("02")
PROTOCOL.STATUS = bytes.fromhex("01")
PROTOCOL.PLACEHOLDER1 = bytes.fromhex("00")
PROTOCOL.PLACEHOLDER2 = bytes.fromhex("0000") # sensor type and double crc
PROTOCOL.PLACEHOLDER3 = bytes.fromhex("00000000") # offset time
PROTOCOL.PLACEHOLDER4 = bytes.fromhex("000000000000000000000000") # delivery|poweron|offset time
PROTOCOL.START = bytes.fromhex("A5")
PROTOCOL.END = bytes.fromhex("15")
PROTOCOL.ALT_FRAME_START = bytes.fromhex("AA")

class FrameError(Exception):
    """Frame Validation Error"""

def log_call(prefix: str):
    def decorator(f):
        @wraps(f)
        async def wrapper(*args, **kwargs):
            _LOGGER.debug(f"[{args[0].host}] {prefix}{f': {format(args[1])}' if len(args) > 1 else ''}")
            return await f(*args, **kwargs)
        return wrapper
    return decorator

def log_return(prefix: str):
    def decorator(f):
        @wraps(f)
        async def wrapper(*args, **kwargs):
            r = await f(*args, **kwargs)
            _LOGGER.debug(f"[{args[0].host}] {prefix}: {format(r)}")
            return r
        return wrapper
    return decorator

class Solarman:
    def __init__(self, host: str, port: int | str, transport: str, serial: int, slave: int, timeout: int):
        self.host = host
        self.port = port
        self.transport = transport
        self.serial = serial
        self.slave = slave
        self.timeout = timeout

        self._keeper: asyncio.Task | None = None
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._semaphore = asyncio.Semaphore(1)
        self._data_queue = asyncio.Queue(maxsize = 1)
        self._data_event = Event()
        self._last_frame: bytes | None = None
        self.relay_values = []

    @staticmethod
    def _get_response_code(code: int) -> int:
        return code - 0x30

    @staticmethod
    def _calculate_checksum(frame: bytes) -> int:
        checksum = 0
        for d in frame:
            checksum += d & 0xFF
        return int(checksum & 0xFF)

    @property
    def serial(self) -> int:
        return self._serial

    @serial.setter
    def serial(self, value: int | bytes) -> None:
        if isinstance(value, int):
            self._serial = value
            self.serial_bytes = struct.pack("<I", value) if value > 0 else PROTOCOL.PLACEHOLDER3
        else:
            self._serial = int.from_bytes(value, "little")
            self.serial_bytes = value

    @property
    def transport(self) -> str:
        return self._transport

    @transport.setter
    def transport(self, value: str) -> None:
        self._transport = value
        if value == "tcp":
            self._get_response = self._parse_adu_from_sol_response
            self._handle_frame = self._handle_protocol_frame
        else:
            self._get_response = self._parse_adu_from_tcp_response if not value.endswith("rtu") else self._parse_adu_from_rtu_response
            self._handle_frame = None

    @property
    def connected(self):
        return self._keeper and not self._keeper.done()

    @property
    def sequence_number(self) -> int:
        self._sequence_number = ((self._sequence_number + 1) & 0xFF) if hasattr(self, "_sequence_number") else randrange(0x01, 0xFF)
        return self._sequence_number

    def _protocol_header(self, length: int, control: int, seq: bytes) -> bytearray:
        return bytearray(PROTOCOL.START
            + struct.pack("<H", length)
            + PROTOCOL.CONTROL_CODE_SUFFIX
            + struct.pack("<B", control)
            + seq
            + self.serial_bytes)

    def _protocol_trailer(self, frame: bytes) -> bytearray:
        return bytearray(struct.pack("<B", self._calculate_checksum(frame[1:])) + PROTOCOL.END)

    def _handle_alt_aa_frame(self, frame: bytes) -> None:
        try:
            if len(frame) < 5:
                _LOGGER.warning(f"[{self.host}] Zu kurzer AA-Frame: {frame.hex(' ')}")
                return
            address = frame[1]
            function_code = frame[2]
            register = int.from_bytes(frame[3:5], "big")
            data_bytes = frame[5:]
            registers = [int.from_bytes(data_bytes[i:i+2], "big") for i in range(0, len(data_bytes), 2) if i + 2 <= len(data_bytes)]
            self.relay_values = registers
            _LOGGER.info(f"[{self.host}] AA-Telegramm → Device: {address}, Function: {function_code:#02x}, Start-Reg: {register:#04x}, Values: {registers}")
        except Exception as e:
            _LOGGER.warning(f"[{self.host}] Error processing AA frame: {e!r}")

    def _received_frame_is_valid(self, frame: bytes) -> bool:
        if frame.startswith(PROTOCOL.START):
            if frame[5] != self._sequence_number:
                if frame[4] == PROTOCOL.CONTROL_CODE.REQUEST and len(frame) > 6:
                    f = int.from_bytes(frame[5:6], "big") == len(frame[6:])
                    if len(frame) > 9:
                        f &= int.from_bytes(frame[8:9], "big") == len(frame[9:])
                    if f:
                        _LOGGER.debug(f"[{self.host}] TCP_DETECTED: {frame.hex(' ')}")
                        self.transport = "modbus_tcp"
                        return True
                _LOGGER.debug(f"[{self.host}] SEQ_MISMATCH: {frame.hex(' ')}")
                return False
            if not frame.endswith(PROTOCOL.END):
                _LOGGER.debug(f"[{self.host}] PROTOCOL_MISMATCH: {frame.hex(' ')}")
                return False
            return True
        elif frame.startswith(PROTOCOL.ALT_FRAME_START):
            _LOGGER.debug(f"[{self.host}] ALT_FRAME_DETECTED (AA): {frame.hex(' ')}")
            self._handle_alt_aa_frame(frame)
            return False
        else:
            _LOGGER.debug(f"[{self.host}] PROTOCOL_MISMATCH: {frame.hex(' ')}")
            return False

    def _received_frame_response(self, frame: bytes) -> tuple[bool, bytearray]:
        do_continue = True
        response_frame = None
        if frame[4] != PROTOCOL.CONTROL_CODE.REQUEST and frame[4] in PROTOCOL.CONTROL_CODES:
            do_continue = False
            # Maybe do_continue = True for CONTROL_CODE.DATA|INFO|REPORT and thus process packets in the future?
            control_name = [i for i in PROTOCOL.CONTROL_CODE.__dict__ if PROTOCOL.CONTROL_CODE.__dict__[i] == frame[4]][0]
            _LOGGER.debug(f"[{self.host}] PROTOCOL_{control_name} RECV: {frame.hex(" ")}")
            response_frame = self._protocol_header(10, self._get_response_code(frame[4]), frame[5:7]) + bytearray(PROTOCOL.PLACEHOLDER1 # Frame Type
                + PROTOCOL.STATUS
                + struct.pack("<I", int(time.time()))
                + PROTOCOL.PLACEHOLDER3) # Offset?
            response_frame[5] = (response_frame[5] + 1) & 0xFF
            response_frame += self._protocol_trailer(response_frame)
            _LOGGER.debug(f"[{self.host}] PROTOCOL_{control_name} SENT: {response_frame.hex(" ")}")
        return do_continue, response_frame

    async def _write(self, data: bytes) -> None:
        try:
            self._writer.write(data)
            await self._writer.drain()
        except AttributeError as e:
            raise ConnectionError("Connection is closed") from e
        except OSError as e:
            raise TimeoutError("Peer is unreachable") from e
        except Exception as e:
            raise e

    async def _handle_protocol_frame(self, frame):
        if (do_continue := self._received_frame_is_valid(frame)):
            do_continue, response_frame = self._received_frame_response(frame)
            if response_frame is not None:
                await self._write(response_frame)
        return do_continue

    async def _keeper_loop(self) -> None:
        try:
            while True:
                try:
                    data = await self._reader.read(1024)
                except (ConnectionError, ConnectionResetError, asyncio.IncompleteReadError, OSError) as e:
                    _LOGGER.debug(f"[{self.host}] Read error: {e!r}. Will restart the connection.")
                    break

                if data == b"":
                    _LOGGER.debug(f"[{self.host}] Empty response in keeper loop. Connection closed by the peer. Will restart the connection.")
                    break

                if self._handle_frame is not None:
                    try:
                        handled = await self._handle_frame(data)
                        if not handled:
                            continue  # Skip to next loop iteration
                    except Exception as e:
                        _LOGGER.warning(f"[{self.host}] Exception in frame handler: {e!r}")
                        continue

                if not self._data_event.is_set():
                    _LOGGER.debug(f"[{self.host}] Late data received — skipping")
                    continue

                if not self._data_queue.empty():
                    _ = self._data_queue.get_nowait()
                self._data_queue.put_nowait(data)
                self._data_event.clear()
        finally:
            await self._close()

            # Reconnect — spawn a new connection+keeper if needed
            _LOGGER.info(f"[{self.host}] Reconnecting after disconnect")
            await asyncio.sleep(1)
            await self._open_connection()  # let that one create a new keeper

    @throttle(0.2)
    async def _open_connection(self) -> None:

        retries = 0
        backoff = INITIAL_BACKOFF

        while retries < MAX_RETRIES:
            try:
                _LOGGER.debug(f"[{self.host}] Attempting connection (try {retries + 1}/{MAX_RETRIES})...")
                self._reader, self._writer = await asyncio.wait_for(
                    asyncio.open_connection(self.host, self.port),
                    self.timeout
                )
                self._keeper = asyncio.create_task(self._keeper_loop())
                if self._keeper:
                    _LOGGER.debug(f"[{self.host}] Created a new connection keeper: {self._keeper.get_name()}")

                if self._data_event.is_set():
                    _LOGGER.debug(f"[{self.host}] Successful reconnection! Data expected. Retrying last request.")
                    await self._write(self._last_frame)
                else:
                    _LOGGER.debug(f"[{self.host}] Connected successfully.")
                return  # Success

            except Exception as e:
                _LOGGER.debug(f"[{self.host}] Connection attempt failed: {e!r}")

                retries += 1
                if retries >= MAX_RETRIES or self._last_frame is None:
                    raise ConnectionError(f"[{self.host}] Cannot open connection after {retries} attempts.") from e

                _LOGGER.debug(f"[{self.host}] Retrying in {backoff} seconds...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)

        # Extra safeguard (should not be reached)
        raise ConnectionError(f"[{self.host}] Exhausted retries without success.")

    async def _close(self) -> None:
        if self._writer is not None:
            try:
                # Trigger write machinery and possible errors without sending actual data
                await self._write(b"")
            except (ConnectionError, TimeoutError) as e:
                _LOGGER.debug(f"[{self.host}] Ignored during close (write): {e!r}")
            except Exception as e:
                _LOGGER.debug(f"[{self.host}] Unexpected error during close (write): {e!r}")
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except (AttributeError, OSError) as e:
                # Happens if socket is already half-broken or not properly initialized
                _LOGGER.debug(f"[{self.host}] Ignored during close (close/wait_closed): {e!r}")
            except Exception as e:
                _LOGGER.debug(f"[{self.host}] Unexpected error during close (write): {e!r}")
            finally:
                self._writer = None

        if self._reader is not None:
            self._reader = None

        if hasattr(self, "_keeper") and self._keeper:
            _LOGGER.debug(f"[{self.host}] Closing connection keeper: {self._keeper.get_name()}")
            if not self._keeper.done():
                self._keeper.cancel()
                try:
                    await self._keeper
                except asyncio.CancelledError:
                    _LOGGER.debug(f"[{self.host}] Keeper task cancelled")
                except Exception as e:
                    _LOGGER.warning(f"[{self.host}] Error while cancelling keeper task: {e!r}")
            self._keeper = None

        _LOGGER.debug(f"[{self.host}] Connection closed and cleaned up.")

    @throttle(0.1)
    @log_call("SENT")
    @log_return("RECV")
    async def _send_receive_frame(self, frame: bytes) -> bytes:
        if not self._writer:
            if not self.connected:
                self._keeper = create_task(self._open_connection())
            await self._keeper

        self._data_event.set()
        self._last_frame = frame

        try:
            await self._write(frame)
            read_timeout = self.timeout * 3 - 1
            while True:
                try:
                    return await asyncio.wait_for(self._data_queue.get(), read_timeout)
                except TimeoutError:
                    _LOGGER.debug(f"[{self.host}] Timeout while receiving in _send_receive_frame after {read_timeout} seconds.")
                    await self._close()
        finally:
            self._data_event.clear()

    async def _parse_adu_from_sol_response(self, code: int, address: int, **kwargs) -> list[int]:
        async def _get_sol_response(frame: bytes) -> bytes:
            request_frame = self._protocol_header(15 + len(frame),
                PROTOCOL.CONTROL_CODE.REQUEST,
                struct.pack("<H", self.sequence_number)
            ) + bytearray(PROTOCOL.FRAME_TYPE
                + PROTOCOL.PLACEHOLDER2 # sensor type
                + PROTOCOL.PLACEHOLDER4 # delivery|poweron|offset time
                + frame)
            return await self._send_receive_frame(request_frame + self._protocol_trailer(request_frame))
        req = rtu.function_code_to_function_map[code](self.slave, address, **kwargs)
        res = await _get_sol_response(req)
        if self.serial_bytes == PROTOCOL.PLACEHOLDER3:
            self.serial = res[7:11]
            _LOGGER.debug(f"[{self.host}] SERIAL_SET: {self.serial}")
            res = await _get_sol_response(req)
        if res[4] != self._get_response_code(PROTOCOL.CONTROL_CODE.REQUEST):
            raise FrameError("Invalid control code")
        if res[5] != self._sequence_number:
            raise FrameError("Invalid sequence number")
        if res[11:12] != PROTOCOL.FRAME_TYPE:
            _LOGGER.debug(f"[{self.host}] UNEXPECTED_FRAME_TYPE: {int.from_bytes(res[11:12])}")
        if res[-2] != self._calculate_checksum(res[1:-2]):
            raise FrameError("Invalid checksum")
        res = res[25:-2]
        if len(res) < 5: # Short version of modbus exception (undocumented)
            if len (res) > 0 and (modbusError := error_code_to_exception_map.get(res[0])):
                raise modbusError()
            raise FrameError("Invalid modbus frame")
        if res.endswith(PROTOCOL.PLACEHOLDER2) and get_crc(res[:-4]) == res[-4:-2]: # Double CRC (XXXX0000) correction
            res = res[:-2]
        return rtu.parse_response_adu(res, req)

    async def _parse_adu_from_rtu_response(self, code: int, address: int, **kwargs) -> list[int]:
        req = rtu.function_code_to_function_map[code](self.slave, address, **kwargs)
        return rtu.parse_response_adu(await self._send_receive_frame(req), req)

    async def _parse_adu_from_tcp_response(self, code: int, address: int, **kwargs) -> list[int]:
        req = tcp.function_code_to_function_map[code](self.slave, address, **kwargs)
        res = await self._send_receive_frame(req)
        if 8 <= len(res) <= 10: # Incomplete response correction
            res = res[:5] + b'\x06' + res[6:] + (req[len(res):10] if len(req) > 12 else (b'\x00' * (10 - len(res)))) + b'\x00\x01'
        return tcp.parse_response_adu(res, req)

    @retry()
    async def get_response(self, code: int, address: int, **kwargs) -> list[int]:
        return await self._get_response(code, address, **kwargs)

    @log_return("DATA")
    async def execute(self, code: int, address: int, **kwargs) -> list[int]:
        if code not in FUNCTION_CODES:
            raise Exception(f"Invalid modbus function code {code:02}")

        async with asyncio.timeout(self.timeout * 6):
            async with self._semaphore:
                return await self.get_response(code, address, **kwargs)

    @log_call("Closing connection")
    async def close(self) -> None:
        async with self._semaphore:
            if self.connected:
                self._keeper.cancel()
                try:
                  await self._keeper
                except asyncio.CancelledError:
                  _LOGGER.debug(f"[{self.host}] Keeper task cancelled")
                except Exception as e:
                  _LOGGER.warning(f"[{self.host}] Error while cancelling keeper task: {e!r}")

            self._keeper = None

            await self._close()
