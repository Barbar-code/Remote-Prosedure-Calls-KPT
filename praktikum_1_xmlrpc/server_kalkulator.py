from __future__ import annotations

import argparse
import math
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer


class ThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    daemon_threads = True


def tambah(a: int | float, b: int | float) -> int | float:
    return a + b


def kali(a: int | float, b: int | float) -> int | float:
    return a * b


def bagi(a: int | float, b: int | float) -> float:
    if b == 0:
        raise ValueError("Pembagian dengan nol")
    return a / b


def pangkat(a: int | float, b: int | float) -> int | float:
    return a**b


def akar(x: int | float) -> float:
    if x < 0:
        raise ValueError("Akar bilangan negatif tidak didukung")
    return math.sqrt(x)


def statistik(daftar: list[int | float]) -> dict[str, int | float]:
    if not daftar:
        raise ValueError("Daftar angka tidak boleh kosong")
    return {"min": min(daftar), "max": max(daftar), "rata2": sum(daftar) / len(daftar)}


def echo_datetime(value: object) -> object:
    return value


def create_server(
    host: str = "127.0.0.1",
    port: int = 8000,
    log_requests: bool = True,
) -> ThreadedXMLRPCServer:
    server = ThreadedXMLRPCServer((host, port), allow_none=True, logRequests=log_requests)
    server.register_introspection_functions()
    server.register_function(tambah, "tambah")
    server.register_function(kali, "kali")
    server.register_function(bagi, "bagi")
    server.register_function(pangkat, "pangkat")
    server.register_function(akar, "akar")
    server.register_function(statistik, "statistik")
    server.register_function(echo_datetime, "echo_datetime")
    return server


def main() -> None:
    parser = argparse.ArgumentParser(description="Server kalkulator XML-RPC")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    server = create_server(args.host, args.port)
    print(f"Server XML-RPC berjalan di {args.host}:{args.port}...")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer dihentikan.")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
