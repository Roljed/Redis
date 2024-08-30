import socket


ADDRESS = "localhost"
REDIS_PORT = 6379
PONG = "+PONG\r\n"


def main():
    server_socket = socket.create_server((ADDRESS, REDIS_PORT), reuse_port=True)
    client_socket, address = server_socket.accept()
    print(f"accepted connection from {address}")
    
    with client_socket:
        # Read data
        data = client_socket.recv(1024)

        # Write data back
        client_socket.sendall(PONG.encode())

    server_socket.close()


if __name__ == "__main__":
    main()
