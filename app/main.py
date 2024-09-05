import socket
import selectors
from typing import List


ADDRESS = "localhost"
BYTES_SIZE = 1024
REDIS_PORT = 6379
REDIS_RESPONSE_PONG = "+PONG\r\n"


sel = selectors.DefaultSelector()


class Redis():
    def __init__(self) -> None:
        self.command = []
        self.hash_map = {}

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


    def server_response(self, client_socket: socket, response: str):
        content_len = len(response)
        if not response.startswith("+"):
            response = f"${content_len}\r\n{response}\r\n"
        client_socket.sendall(response.encode())


    def accept(self, server_socket: socket) -> None:
        client_socket, address = server_socket.accept()
        print(f"Accepted connection from {address}")
        client_socket.setblocking(False)
        sel.register(client_socket, selectors.EVENT_READ, self.read)


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
                self.hash_map[key] = value
                response = "+OK\r\n"
                self.server_response(client_socket, response)
            elif "GET" == main_command and total_commands > 1:
                key = self.command[1]
                value = self.hash_map.get(key)
                if not value:
                    value = "$-1\r\n"
                self.server_response(client_socket, value)
        else:
            sel.unregister(client_socket)
            client_socket.close()


def main():
    server_socket = socket.create_server((ADDRESS, REDIS_PORT), reuse_port=True)
    server_socket.listen()
    server_socket.setblocking(False)

    redis = Redis()

    sel.register(server_socket, selectors.EVENT_READ, redis.accept)

    print(f"Server is listening...")

    while True:
        events = sel.select()
        for key, _ in events:
            callback = key.data
            callback(key.fileobj)


if __name__ == "__main__":
    main()
