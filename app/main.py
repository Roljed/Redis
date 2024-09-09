import socket
import selectors
from datetime import datetime, timedelta
from typing import List


ADDRESS = "localhost"
BYTES_SIZE = 1024
REDIS_PORT = 6379
REDIS_RESPONSE_PONG = "+PONG\r\n"





class Redis():
    def __init__(self) -> None:
        self.sel = None
        self.command = []
        self.hash_map = {}
    
    def server_up(self) -> selectors.DefaultSelector:
        server_socket = socket.create_server((ADDRESS, REDIS_PORT), reuse_port=True)
        server_socket.listen()
        server_socket.setblocking(False)

        self.sel = selectors.DefaultSelector()
        self.sel.register(server_socket, selectors.EVENT_READ, self.accept)

        print(f"Server is listening...")

        return self.sel


    def parse_redis_command(self, command: str) -> None:
        """
        Parses a RESP command from the client
        """
        command_lines = command.splitlines()
        print(f"Command lines:\n{command_lines}")
        data_type = command_lines[0]
        if data_type.startswith("*"):
            num_arg = int(command_lines[0][1:])
            index = 1
            command_parts = []
            for _ in range(num_arg):
                if command_lines[index].startswith("$"):
                    length = int(command_lines[index][1:])
                    index += 1
                    command_parts.append(command_lines[index][:length])
                    index += 1
            self.command = command_parts
            return
        self.command = []


    @staticmethod
    def server_response(client_socket: socket, response: str):
        content_len = len(response)
        if not response.startswith("+"):
            response = f"${content_len}\r\n{response}\r\n"
        client_socket.sendall(response.encode())


    def accept(self, server_socket: socket) -> None:
        client_socket, address = server_socket.accept()
        print(f"Accepted connection from {address}")
        client_socket.setblocking(False)
        self.sel.register(client_socket, selectors.EVENT_READ, self.read)


    def read(self, client_socket: socket) -> None:
        request: bytes = client_socket.recv(BYTES_SIZE)
        if request:
            data: str = request.decode()
            print(f"Received: {data}")

            self.parse_redis_command(data)
            total_commands = len(self.command)
            main_command = self.command[0].upper()
            
            if "PING" == main_command:
                client_socket.sendall(REDIS_RESPONSE_PONG.encode())
            elif "ECHO" == main_command and total_commands > 1:
                echo_response = data.split("\r\n")[-2]
                self.server_response(client_socket, echo_response)
            elif "SET" == main_command and total_commands > 2:
                key = self.command[1]
                value = self.command[2]
                # Expiry key
                if total_commands > 3 and "px" in self.command[3]:
                    expiry = self.command[4]
                    current_time = datetime.now()
                    expiry_time = current_time + timedelta(seconds=int(expiry))
                    self.hash_map[key] = (expiry_time, value)
                self.hash_map[key] = (None, value)
                response = "+OK\r\n"
                self.server_response(client_socket, response)
            elif "GET" == main_command and total_commands > 1:
                key = self.command[1]
                expiry, value = self.hash_map.get(key)
                current_time = datetime.now()
                if not value or (expiry and current_time > expiry):
                    value = "$-1\r\n"
                self.server_response(client_socket, value)
        else:
            sel.unregister(client_socket)
            client_socket.close()


def main():
    redis = Redis()
    server_selector = redis.server_up()

    while True:
        events = server_selector.select()
        for key, _ in events:
            callback = key.data
            callback(key.fileobj)


if __name__ == "__main__":
    main()
