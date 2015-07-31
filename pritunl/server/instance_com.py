from pritunl.server.listener import *
from pritunl.server.clients import *

from pritunl.constants import *
from pritunl.helpers import *
from pritunl import settings
from pritunl import logger
from pritunl import utils
from pritunl import mongo

import os
import time
import datetime
import threading
import socket

class ServerInstanceCom(object):
    def __init__(self, server, instance):
        self.server = server
        self.instance = instance
        self.sock = None
        self.socket_path = instance.management_socket_path
        self.bytes_lock = threading.Lock()
        self.bytes_recv = 0
        self.bytes_sent = 0
        self.client = None
        self.clients = Clients(server, instance, self)
        self.client_bytes = {}
        self.cur_timestamp = utils.now()
        self.bandwidth_rate = settings.vpn.bandwidth_update_rate

    @cached_static_property
    def users_ip_collection(cls):
        return mongo.get_collection('users_ip')

    def client_kill(self, client):
        self.sock.send('client-kill %s\n' % client['client_id'])
        self.push_output('Disconnecting user org_id=%s user_id=%s' % (
            client['org_id'], client['user_id']))

    def send_client_auth(self, client, client_conf):
        self.sock.send('client-auth %s %s\n%s\nEND\n' % (
            client['client_id'], client['key_id'], client_conf))

    def send_client_deny(self, client, reason):
        self.sock.send('client-deny %s %s "%s"\n' % (
            client['client_id'], client['key_id'], reason))
        self.push_output('ERROR User auth failed "%s"' % reason)

    def push_output(self, message):
        self.server.output.push_message(message)

    def parse_bytecount(self, client_id, bytes_recv, bytes_sent):
        _, bytes_recv_prev, bytes_sent_prev = self.client_bytes.get(
            client_id, (None, 0, 0))

        self.client_bytes[client_id] = (
            self.cur_timestamp, bytes_recv, bytes_sent)

        self.bytes_lock.acquire()
        self.bytes_recv += bytes_recv - bytes_recv_prev
        self.bytes_sent += bytes_sent - bytes_sent_prev
        self.bytes_lock.release()

    def parse_line(self, line):
        line_14 = line[:14]
        line_18 = line[:18]

        if self.client:
            if line == '>CLIENT:ENV,END':
                cmd = self.client['cmd']
                if cmd == 'connect':
                    self.clients.connect(self.client)
                elif cmd == 'connected':
                    self.clients.connected(self.client)
                elif cmd == 'disconnected':
                    self.clients.disconnected(self.client)
                self.client = None
            elif line[:11] == '>CLIENT:ENV':
                env_key, env_val = line[12:].split('=', 1)
                if env_key == 'tls_id_0':
                    tls_env = ''.join(x for x in env_val if x in VALID_CHARS)
                    o_index = tls_env.find('O=')
                    cn_index = tls_env.find('CN=')

                    if o_index < 0 or cn_index < 0:
                        self.send_client_deny(self.client,
                            'Failed to parse org_id and user_id')
                        self.client = None
                        return

                    if o_index > cn_index:
                        org_id = tls_env[o_index + 2:]
                        user_id = tls_env[3:o_index]
                    else:
                        org_id = tls_env[2:cn_index]
                        user_id = tls_env[cn_index + 3:]

                    self.client['org_id'] = org_id
                    self.client['user_id'] = user_id
                elif env_key == 'IV_HWADDR':
                    self.client['mac_addr'] = env_val
                elif env_key == 'untrusted_ip':
                    self.client['remote_ip'] = env_val
                elif env_key == 'IV_PLAT':
                    self.client['platform'] = env_val
                elif env_key == 'UV_ID':
                    self.client['device_id'] = env_val
                elif env_key == 'UV_NAME':
                    self.client['device_name'] = env_val
                elif env_key == 'password':
                    self.client['otp_code'] = env_val
        elif line_14 == '>BYTECOUNT_CLI':
            client_id, bytes_recv, bytes_sent = line.split(',')
            client_id = client_id.split(':')[1]
            self.parse_bytecount(client_id, int(bytes_recv), int(bytes_sent))
        elif line_14 in ('>CLIENT:CONNEC', '>CLIENT:REAUTH'):
            _, client_id, key_id = line.split(',')
            self.client = {
                'cmd': 'connect',
                'client_id': client_id,
                'key_id': key_id,
            }
        elif line_18 == '>CLIENT:ESTABLISHE':
            _, client_id = line.split(',')
            self.client = {
                'cmd': 'connected',
                'client_id': client_id,
            }
        elif line_18 == '>CLIENT:DISCONNECT':
            _, client_id = line.split(',')
            self.client = {
                'cmd': 'disconnected',
                'client_id': client_id,
            }

    def wait_for_socket(self):
        for _ in xrange(10000):
            if os.path.exists(self.socket_path):
                return
            time.sleep(0.001)
        logger.error('Server management socket path not found', 'server',
            server_id=self.server.id,
            instance_id=self.instance.id,
            socket_path=self.socket_path,
        )

    def on_msg(self, evt):
        event_type, user_id = evt['message']

        if event_type != 'user_disconnect':
            return

        self.clients.disconnect_user(user_id)

    @interrupter
    def _watch_thread(self):
        try:
            while True:
                self.cur_timestamp = utils.now()
                timestamp_ttl = self.cur_timestamp - datetime.timedelta(
                    seconds=180)

                for client_id, (timestamp, _, _) in self.client_bytes.items():
                    if timestamp < timestamp_ttl:
                        self.client_bytes.pop(client_id, None)

                self.bytes_lock.acquire()
                bytes_recv = self.bytes_recv
                bytes_sent = self.bytes_sent
                self.bytes_recv = 0
                self.bytes_sent = 0
                self.bytes_lock.release()

                if bytes_recv != 0 or bytes_sent != 0:
                    self.server.bandwidth.add_data(
                        utils.now(), bytes_recv, bytes_sent)

                yield interrupter_sleep(self.bandwidth_rate)
                if self.instance.sock_interrupt:
                    return
        except GeneratorExit:
            raise
        except:
            self.push_output('ERROR Management thread error')
            logger.exception('Error in management watch thread', 'server',
                server_id=self.server.id,
                instance_id=self.instance.id,
            )
            self.instance.stop_process()

    def _socket_thread(self):
        try:
            self.connect()

            time.sleep(1)
            self.sock.send('bytecount %s\n' % self.bandwidth_rate)

            add_listener(self.instance.id, self.on_msg)

            data = ''
            while True:
                data += self.sock.recv(SOCKET_BUFFER)
                if not data:
                    if not self.instance.sock_interrupt and \
                            not check_global_interrupt():
                        self.instance.stop_process()
                        self.push_output(
                            'ERROR Management socket exited unexpectedly')
                        logger.error('Management socket exited unexpectedly')
                    return
                lines = data.split('\n')
                data = lines.pop()
                for line in lines:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        self.parse_line(line)
                    except:
                        logger.exception('Failed to parse line from vpn com',
                            'server',
                            server_id=self.server.id,
                            instance_id=self.instance.id,
                            line=line,
                        )
        except:
            if not self.instance.sock_interrupt:
                self.push_output('ERROR Management socket exception')
                logger.exception('Error in management socket thread', 'server',
                    server_id=self.server.id,
                    instance_id=self.instance.id,
                )
            self.instance.stop_process()
        finally:
            remove_listener(self.instance.id)

    def connect(self):
        self.wait_for_socket()
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(self.socket_path)

    def start(self):
        thread = threading.Thread(target=self._socket_thread)
        thread.daemon = True
        thread.start()

        thread = threading.Thread(target=self._watch_thread)
        thread.daemon = True
        thread.start()

        thread = threading.Thread(target=self.clients.ping_thread)
        thread.daemon = True
        thread.start()
