import heapq
import socket
from time import sleep

HEADER_SIZE = 8
MTU = 1472
DATA_SIZE = MTU - HEADER_SIZE
MAX_RETRY = 128
TIMEOUT = 0.0001
RTT = 0.0005


class UDPBasedProtocol:
    def __init__(self, *, local_addr, remote_addr):
        self.udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.remote_addr = remote_addr
        self.udp_socket.bind(local_addr)

    def sendto(self, data):
        return self.udp_socket.sendto(data, self.remote_addr)

    def recvfrom(self, n):
        msg, addr = self.udp_socket.recvfrom(n)
        return msg

    def close(self):
        self.udp_socket.close()


class TCPHeader:
    def __init__(self, seq, ack):
        self.seq = seq
        self.ack = ack

    def __bytes__(self):
        return self.seq.to_bytes(4, 'big') + self.ack.to_bytes(4, 'big')

    @classmethod
    def from_bytes(cls, data):
        seq = int.from_bytes(data[0:4], 'big')
        ack = int.from_bytes(data[4:8], 'big')
        return cls(seq, ack)


class TCPPacket:
    def __init__(self, seq, ack, data):
        self.header = TCPHeader(seq, ack)
        self.data = data

    def __lt__(self, other):
        return self.header.seq < other.header.seq

    def __bytes__(self):
        return bytes(self.header) + self.data

    @classmethod
    def from_bytes(cls, src):
        header = TCPHeader.from_bytes(src[:HEADER_SIZE])
        data = src[HEADER_SIZE:]
        return cls(header.seq, header.ack, data)


class MyTCPProtocol(UDPBasedProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.udp_socket.settimeout(TIMEOUT)
        self.seq = 1
        self.ack = 1
        self.disorder_packets = []
        self.buffer = b''

    def send_ack(self):
        header = TCPHeader(0, self.ack)
        self.sendto(bytes(header))

    def process_disorder(self):
        while len(self.disorder_packets) > 0 and self.disorder_packets[0].header.seq <= self.ack:
            packet = heapq.heappop(self.disorder_packets)
            if packet.header.seq == self.ack:
                self.buffer += packet.data
                self.ack += len(packet.data)

    def process_seq_packet(self, packet):
        if packet.header.seq == self.ack:
            self.buffer += packet.data
            self.ack += len(packet.data)
            self.process_disorder()
        else:
            heapq.heappush(self.disorder_packets, packet)

    def send(self, data: bytes):
        old_seq = self.seq
        size = len(data)

        i = 0
        while i < size:
            block = data[i: i + DATA_SIZE]
            packet = TCPPacket(self.seq + i, self.ack, block)
            self.sendto(bytes(packet))
            i += DATA_SIZE
        sleep(RTT)

        retry = 0
        while self.seq < old_seq + size:
            # logging.warning(f'send {self.seq} {self.ack} {len(self.buffer)}')

            try:
                packet = TCPPacket.from_bytes(self.recvfrom(MTU))
                retry = 0
                if packet.header.seq > 0:
                    self.process_seq_packet(packet)
                    self.send_ack()
                    continue

                if packet.header.ack <= self.seq:
                    continue

                self.seq = packet.header.ack

            except Exception:
                retry += 1
                # if retry > MAX_RETRY:
                #     break

                if self.seq < old_seq + size:
                    block = data[self.seq - old_seq:self.seq - old_seq + DATA_SIZE]
                    packet = TCPPacket(self.seq, self.ack, block)
                    self.sendto(bytes(packet))
                sleep(RTT)

        self.seq = old_seq + size
        return size


    def recv(self, n: int):
        end_ack = self.ack + n - len(self.buffer)
        while self.ack < end_ack:
            # logging.warning(f'recv {self.seq} {self.ack} {len(self.buffer)}')
            try:
                self.process_disorder()
                packet = TCPPacket.from_bytes(self.recvfrom(MTU))
                if packet.header.seq < self.ack:
                    continue
                self.process_seq_packet(packet)

            except Exception:
                self.send_ack()
        self.send_ack()

        data = self.buffer[:n]
        self.buffer = self.buffer[n:]
        return data

    def close(self):
        super().close()
