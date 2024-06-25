from socket import socket as _socket
import socketserver
import sys
import threading
import signal
import os
import socket
from bitcaskdb import BitcaskDatabase
from socketserver import BaseRequestHandler
from typing import Any, Callable

class BitcaskServer(socketserver.ThreadingTCPServer):
    def __init__(self,
                 server_address: tuple[str | bytes | bytearray, int],
                 RequestHandlerClass: Callable[[Any, Any, 'BitcaskServer'], BaseRequestHandler],
                 bind_and_activate: bool = True) -> None:
        super().__init__(server_address, RequestHandlerClass, bind_and_activate)
        self.shutdown_event = threading.Event()
        self.database = BitcaskDatabase()
        self.server_thread = threading.Thread(
            target=self._run_server_handler, 
            name=self._run_server_handler.__name__
        )
        self.allow_reuse_address = False # expirement with this 
        self.request_queue_size = 1024
        self.socket_type = socket.SOCK_STREAM  # explicit TCP specifier
        if sys.version_info.major == 3 and sys.version_info.minor >= 7:
            self.daemon_threads = False  # wait until the daemonic thread complete
            self.block_on_close = True   # wait until the non-daemonic thread also complete

    def shutdown_handler(self, signum, _frame):
        print(f">> Received {signum} signal, shutting down server...")
        self.shutdown_event.set()
        os._exit(0)

    def _run_server_handler(self):
        while not self.shutdown_event.is_set():
            self.handle_request()

    def serve(self) -> threading.Thread:
        signal.signal(signal.SIGINT, self.shutdown_handler)
        signal.signal(signal.SIGTERM, self.shutdown_handler)
        self.server_thread.daemon = True
        self.server_thread.start()
        return self.server_thread

    def server_close(self):
        self.shutdown_event.set()
        super().server_close()

class RequestHandler(socketserver.BaseRequestHandler):
    def __init__(self, request, client_address, server):
        self.database :BitcaskDatabase = server.database
        super().__init__(request, client_address, server)

    def handle(self):
        self.data = self.request.recv(1024).strip().decode('utf-8')
        print(f"Received from {self.client_address[0]}: {self.data}")

        query_parts = list(filter(lambda v: v != "", self.data.split(" ")))
        if not query_parts:
            response = "Error: Invalid command\n"
        else:
            cmd = query_parts[0].upper()
            args = query_parts[1:]
            response = "Error: Invalid command\n"
            try:
                match cmd:
                    case "SET":
                        if len(args) != 2:
                            response = "Error: SET command : SET <key> <value>"
                        else:
                            key, value = args
                            self.database.put(key, value.encode('utf-8'))
                            response = "OK\n"
                    case "GET":
                        if len(args) != 1:
                            response = "Error: GET command: GET <key>"
                        else:
                            key = args[0]
                            value = self.database.get(key)
                            if value is not None:
                                response = value.decode('utf-8')
                            else:
                                response = "Error: Key not found\n"
                    case "DELETE":
                        if len(args) != 1:
                            response = "Error: DELETE command : DELETE <key>"
                        else:
                            key = args[0]
                            self.database.delete(key)
                            response = "OK\n"
                    case _:
                        response = "Error: Invalid command\n"
            except Exception as e:
                response = f"Error: {str(e)}\n"

        self.request.sendall(response.encode('utf-8'))
