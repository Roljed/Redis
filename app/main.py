import socket


ADDRESS = "localhost"
BYTES_SIZE = 512
REDIS_PORT = 6379
PONG = "+PONG\r\n"


def main():
    server_socket = socket.create_server((ADDRESS, REDIS_PORT), reuse_port=True)
    client_socket, address = server_socket.accept()
    print(f"accepted connection from {address}")
    
    while True:
        request: bytes = client_socket.recv(BYTES_SIZE)
        if not request:
            break

        data: str = request.decode()
        if "ping" in data.lower():
            client_socket.sendall(PONG.encode())

    server_socket.close()


if __name__ == "__main__":
    main()
