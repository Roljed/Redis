import socket


ADDRESS = "localhost"
REDIS_PORT = 6379


def main():
    server_socket = socket.create_server((ADDRESS, REDIS_PORT), reuse_port=True)
    server_socket.accept()


if __name__ == "__main__":
    main()
