import socket
import selectors


ADDRESS = "localhost"
BYTES_SIZE = 1024
REDIS_PORT = 6379
PONG = "+PONG\r\n"


sel = selectors.DefaultSelector()


def accept(server_socket):
    client_socket, address = server_socket.accept()
    print(f"Accepted connection from {address}")
    client_socket.setblocking(False)
    sel.register(client_socket, selectors.EVENT_READ, read)


def read(client_socket):
    request: bytes = client_socket.recv(BYTES_SIZE)
    if request:
        data: str = request.decode()
        print(f"Received: {data}")
        if "ping" in data.lower():
            client_socket.sendall(PONG.encode())
        elif "echo" in data.lower():
            echo_response = data.split("\r\n")[-2]
            content_len = len(echo_response)
            response = f"${content_len}\r\n{echo_response}\r\n"
            client_socket.sendall(response.encode())
    else:
        sel.unregister(client_socket)
        client_socket.close()


def main():
    server_socket = socket.create_server((ADDRESS, REDIS_PORT), reuse_port=True)
    server_socket.listen()
    server_socket.setblocking(False)

    sel.register(server_socket, selectors.EVENT_READ, accept)

    print(f"Server is listening...")

    while True:
        events = sel.select()
        for key, mask in events:
            callback = key.data
            callback(key.fileobj)


if __name__ == "__main__":
    main()
