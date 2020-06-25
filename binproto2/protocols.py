import datetime
import math
import random
import struct
import sys
import threading
import time
from collections import deque

import serial

from binproto2.exceptions import (ConnectionLost, FatalError, PayloadOverflow,
                                  ReadTimeout, SynchronizationError)

try:
    import heatshrink
except ImportError:
    import heatshrink2 as heatshrink

try:
    from time import perf_counter
except ImportError:
    from backports.time_perf_counter import perf_counter

def _millis():
    return perf_counter() * 1000

# int.to_bytes is python 3 only. struct.pack is compatible with both 2 and 3.
def _pack_int32(value):
    #return value.to_bytes(4, byteorder='little')
    return struct.pack("<I", value)


def _pack_int16(value):
    #return value.to_bytes(2, byteorder='little')
    return struct.pack("<H", value)


def _pack_int8(value):
    #return value.to_bytes(1, byteorder='little')
    return struct.pack("<B", value)


def _pack_int4_2(vh, vl):
    value = ((vh & 0xF) << 4) | (vl & 0xF)
    #return value.to_bytes(1, byteorder='little')
    return struct.pack("<B", value)


# checksum 16 fletchers
def _build_checksum(buffer):
    def checksum(cs, value):
        cs_low = (((cs & 0xFF) + value) % 255)
        return ((((cs >> 8) + cs_low) % 255) << 8) | cs_low
    cs = 0
    for b in buffer:
        cs = checksum(cs, b)
    return cs


class _TimeOut(object):
    def __init__(self, milliseconds):
        self.duration = milliseconds
        self.reset()

    def reset(self):
        self.endtime = _millis() + self.duration

    def timedout(self):
        return _millis() > self.endtime


class _NoneLogger(object):
    def debug(self, msg, *args, **kwargs):
        pass
    def info(self, msg, *args, **kwargs):
        pass
    def warning(self, msg, *args, **kwargs):
        pass
    def error(self, msg, *args, **kwargs):
        pass
    def critical(self, msg, *args, **kwargs):
        pass
    def exception(self, msg, *args, **kwargs):
        pass


class Protocol(object):
    simerr = float(0)

    def __init__(self, device, baud, bsize, timeout, logger = None):
        self.logger = logger or _NoneLogger()
        self.logger.info("pySerial Version: %s" % serial.VERSION)
        self.port = serial.Serial(device, baudrate = baud, write_timeout = 0, timeout = 1)
        self.device = device
        self.baud = baud
        self.block_size = int(bsize)
        self.simulate_errors = max(min(Protocol.simerr, 1.0), 0.0)
        self.connected = True
        self.response_timeout = timeout
        self.max_block_size = 0
        self.errors = 0
        self.sync = 0
        self.synchronized = False
        self.responses = deque()
        self.applications = []

        self.register(['ok', 'rs', 'ss', 'fe'], self._process_input)

        self.worker_thread = threading.Thread(target=Protocol._receive_worker, args=(self,))
        self.worker_thread.start()

    def register(self, tokens, callback):
        self.applications.append((tokens, callback))

    def shutdown(self):
        self.connected = False
        self.worker_thread.join()
        self.port.close()

    def send_ascii(self, data):
        def await_response_ascii():
            timeout = _TimeOut(self.response_timeout)
            while not len(self.responses):
                time.sleep(0.00001)
                if timeout.timedout():
                    raise ReadTimeout()
            self.responses.popleft()
            self.packet_status = 1

        self.packet_transit = bytearray(data, 'utf8') + b'\n'
        self.packet_status = 0
        self.transmit_attempt = 0

        timeout = _TimeOut(self.response_timeout * 20)
        while self.packet_status == 0:
            try:
                if timeout.timedout():
                    return
                self.port.write(self.packet_transit)
                await_response_ascii()
            except ReadTimeout:
                self.errors += 1
                self.logger.debug("Packetloss detected")
            except serial.serialutil.SerialException:
                return
        self.packet_transit = None    

    def connect(self):
        self.logger.info("Connecting: Switching Marlin to binary protocol...")
        self.send_ascii("M28B1")
        self._send(0, 1)

    def disconnect(self):
        self._send(0, 2)
        self.synchronized = False

    def _receive_worker(self):
        while self.port.in_waiting:
            self.port.reset_input_buffer()

        def dispatch(data):
            for tokens, callback in self.applications:
                for token in tokens:
                    if token == data[:len(token)]:
                        callback((token, data[len(token):]))
                        return

        def reconnect():
            self.logger.info("Reconnecting...")
            self.port.close()
            for _ in range(10):
                try:
                    if self.connected:
                        self.port = serial.Serial(self.device, baudrate = self.baud, write_timeout = 0, timeout = 1)
                        return
                    else:
                        self.logger.info("Connection closed")
                        return
                except:
                    time.sleep(1)
            raise ConnectionLost()

        while self.connected:
            try:
                data = self.port.readline().decode('utf8').rstrip()
                if len(data):
                    self.logger.debug(data)
                    dispatch(data)
            except OSError:
                reconnect()
            except UnicodeDecodeError:
                # dodgy client output or datastream corruption
                self.port.reset_input_buffer()

    def _process_input(self, data):
        self.logger.debug(data)
        self.responses.append(data)

    def _send(self, protocol, packet_type, data = bytearray()):
        self.packet_transit = self._build_packet(protocol, packet_type, data)
        self.packet_status = 0
        self.transmit_attempt = 0

        timeout = _TimeOut(self.response_timeout * 20)
        while self.packet_status == 0:
            try:
                if timeout.timedout():
                    raise ConnectionLost()
                self._transmit_packet(self.packet_transit)
                self._await_response()
            except ReadTimeout:
                self.errors += 1
                self.logger.debug("Packet loss detected")
        self.packet_transit = None

    def _await_response(self):
        timeout = _TimeOut(self.response_timeout)
        while not len(self.responses):
            time.sleep(0.00001)
            if timeout.timedout():
                raise ReadTimeout()

        while len(self.responses):
            token, data = self.responses.popleft()
            switch = {
                'ok' : self._response_ok,
                'rs' : self._response_resend,
                'ss' : self._response_stream_sync,
                'fe' : self._response_fatal_error}
            switch[token](data)

    def _transmit_packet(self, packet):
        def corrupt_array(data):
            rid = random.randint(0, len(data) - 1)
            data[rid] ^= 0xAA
            return data

        packet = bytearray(packet)
        if(self.simulate_errors > 0 and random.random() > (1.0 - self.simulate_errors)):
            if random.random() > 0.9:
                #random data drop
                start = random.randint(0, len(packet))
                end = start + random.randint(1, 10)
                packet = packet[:start] + packet[end:]
                self.logger.debug("Dropping {0} bytes".format(end - start))
            else:
                #random corruption
                packet = corrupt_array(packet)
                self.logger.debug("Single byte corruption")
        self.port.write(packet)
        self.transmit_attempt += 1

    def _build_packet(self, protocol, packet_type, data = bytearray()):
        PACKET_TOKEN = 0xB5AD

        if len(data) > self.max_block_size:
            raise PayloadOverflow()

        packet_buffer = bytearray()

        packet_buffer += _pack_int8(self.sync)                           # 8bit sync id
        packet_buffer += _pack_int4_2(protocol, packet_type)             # 4 bit protocol id, 4 bit packet type
        packet_buffer += _pack_int16(len(data))                          # 16bit packet length
        packet_buffer += _pack_int16(_build_checksum(packet_buffer))     # 16bit header checksum

        if len(data):
            packet_buffer += data
            packet_buffer += _pack_int16(_build_checksum(packet_buffer))

        packet_buffer =  _pack_int16(PACKET_TOKEN) + packet_buffer       # 16bit start token, not included in checksum
        return packet_buffer

    def _response_ok(self, data):
        try:
            packet_id = int(data)
        except ValueError:
            return
        if packet_id != self.sync:
            raise SynchronizationError()
        self.sync = (self.sync + 1) % 256
        self.packet_status = 1

    def _response_resend(self, data):
        packet_id = int(data)
        self.errors += 1
        if not self.synchronized:
            self.logger.info("Retrying synchronization")
        elif packet_id != self.sync:
            raise SynchronizationError()

    def _response_stream_sync(self, data):
        sync, max_block_size, protocol_version = data.split(',')
        self.sync = int(sync)
        self.max_block_size = int(max_block_size)
        self.block_size = self.max_block_size if self.max_block_size < self.block_size else self.block_size
        self.protocol_version = protocol_version
        self.packet_status = 1
        self.synchronized = True
        self.logger.info("Connection synced [{0}], binary protocol version {1}, {2} byte payload buffer".format(self.sync, self.protocol_version, self.max_block_size))

    def _response_fatal_error(self, data):
        raise FatalError()


class FileTransferProtocol(object):
    protocol_id = 1

    class Packet(object):
        QUERY = 0
        OPEN  = 1
        CLOSE = 2
        WRITE = 3
        ABORT = 4

    def __init__(self, protocol, timeout = None, logger = None):
        self.logger = logger or _NoneLogger()
        protocol.register(['PFT:success', 'PFT:version:', 'PFT:fail', 'PFT:busy', 'PFT:ioerror', 'PTF:invalid'], self._process_input)
        self.protocol = protocol
        self.response_timeout = timeout or protocol.response_timeout
        self.responses = deque()

    def connect(self):
        self.protocol._send(FileTransferProtocol.protocol_id, FileTransferProtocol.Packet.QUERY)

        token, data = self._await_response()
        if token != 'PFT:version:':
            return False

        self.version, _, compression = data.split(':')
        if compression != 'none':
            algorithm, window, lookahead = compression.split(',')
            self.compression = {'algorithm': algorithm, 'window': int(window), 'lookahead': int(lookahead)}
        else:
            self.compression = {'algorithm': 'none'}

        self.logger.info("File Transfer version: {0}, compression: {1}".format(self.version, self.compression['algorithm']))

    def open(self, filename, compression, dummy):
        payload =  b'\1' if dummy else b'\0'          # dummy transfer
        payload += b'\1' if compression else b'\0'    # payload compression
        payload += bytearray(filename, 'utf8') + b'\0'# target filename + null terminator

        timeout = _TimeOut(5000)
        token = None
        self.protocol._send(FileTransferProtocol.protocol_id, FileTransferProtocol.Packet.OPEN, payload)
        while token != 'PFT:success' and not timeout.timedout():
            try:
                token, _ = self._await_response(1000)
                if token == 'PFT:success':
                    self.logger.info("Opened file: {0}".format(filename))
                    return
                elif token == 'PFT:busy':
                    self.logger.info("Broken transfer detected, purging")
                    self.abort()
                    time.sleep(0.1)
                    self.protocol._send(FileTransferProtocol.protocol_id, FileTransferProtocol.Packet.OPEN, payload)
                    timeout.reset()
                elif token == 'PFT:fail':
                    raise Exception("Cannot open file on client")
            except ReadTimeout:
                pass
        raise ReadTimeout()

    def write(self, data):
        self.protocol._send(FileTransferProtocol.protocol_id, FileTransferProtocol.Packet.WRITE, data)

    def close(self):
        self.protocol._send(FileTransferProtocol.protocol_id, FileTransferProtocol.Packet.CLOSE)
        token, _ = self._await_response(1000)
        if token == 'PFT:success':
            self.logger.info("File closed")
            return
        elif token == 'PFT:ioerror':
            self.logger.warning("Client storage device IO error")
        elif token == 'PFT:invalid':
            self.logger.warning("No open file")

    def abort(self):
        self.protocol._send(FileTransferProtocol.protocol_id, FileTransferProtocol.Packet.ABORT)
        token, _ = self._await_response()
        if token == 'PFT:success':
            self.logger.info("Transfer aborted")

    def copy(self, filename, dest_filename, compression, dummy):
        self.connect()

        compression_support = self.compression['algorithm'] == 'heatshrink' and compression
        if compression and not self.compression['algorithm'] == 'heatshrink':
            self.logger.warning("Compression not supported by client")

        data = open(filename, "rb").read()
        filesize = len(data)

        self.open(dest_filename, compression_support, dummy)

        block_size = self.protocol.block_size
        if compression_support:
            data = heatshrink.encode(data, window_sz2=self.compression['window'], lookahead_sz2=self.compression['lookahead'])

        cratio = filesize / len(data)

        # int() ensures compat with python 2
        blocks = int(math.floor((len(data) + block_size - 1) / block_size))
        kibs = 0
        start_time = _millis()
        for i in range(blocks):
            start = block_size * i
            end = start + block_size
            self.write(data[start:end])
            kibs = (( (i+1) * block_size) / 1024) / (_millis() + 1 - start_time) * 1000
            self.logger.info("\rPROGRESS: {0:2.2f}% {1:4.2f}KiB/s {2} Errors: {3}".format((i / blocks) * 100, kibs, "[{0:4.2f}KiB/s]".format(kibs * cratio) if compression_support else "", self.protocol.errors))
        self.logger.info("\rPROGRESS: {0:2.2f}% {1:4.2f}KiB/s {2} Errors: {3}".format(100, kibs, "[{0:4.2f}KiB/s]".format(kibs * cratio) if compression_support else "", self.protocol.errors)) # no one likes transfers finishing at 99.8%

        self.close()

        self.logger.info("Transfer complete")

    def _process_input(self, data):
        self.logger.debug(data)
        self.responses.append(data)

    def _await_response(self, timeout = None):
        timeout = _TimeOut(timeout or self.response_timeout)
        while not len(self.responses):
            time.sleep(0.0001)
            if timeout.timedout():
                raise ReadTimeout()

        return self.responses.popleft()


class EchoProtocol(object):
    def __init__(self, protocol, logger):
        self.logger = logger or _NoneLogger()
        protocol.register(['echo:'], self._process_input)
        self.protocol = protocol

    def _process_input(self, data):
        self.logger.info(data)
