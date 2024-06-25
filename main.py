import time
from bitcaskserver.server import BitcaskServer, RequestHandler


if __name__ == "__main__":
    HOST,PORT = ("127.0.0.1",9090)

    with BitcaskServer((HOST, PORT), RequestHandler) as srv:
        print(f"Running TCP server on {HOST}:{PORT}")
        server_thread = srv.serve()
        try:
            while not srv.shutdown_event.is_set():
                time.sleep(0.5)
        except KeyboardInterrupt:
            print("Keyboard interrupt received, shutting down.")
        finally:
            srv.shutdown_event.set()
            server_thread.join(timeout=0.5)
            srv.server_close()
            print("Server shut down gracefully.")
