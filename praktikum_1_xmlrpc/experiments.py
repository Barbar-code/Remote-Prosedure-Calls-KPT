from __future__ import annotations

import argparse
import datetime
import threading
import time
import xmlrpc.client
from xmlrpc.client import ServerProxy

from server_kalkulator import create_server, tambah


def run_experiments() -> None:
    server = create_server(port=0, log_requests=False)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_address[1]}/"
    try:
        with ServerProxy(url, allow_none=True) as proxy:
            print("L1.1 pangkat(2, 10):", proxy.pangkat(2, 10))
            print("L1.1 akar(81):", proxy.akar(81))
            print("L1.2 statistik:", proxy.statistik([4, 7, 9, 12]))
            print("L1.3 datetime:", proxy.echo_datetime(datetime.datetime(2025, 1, 2, 3, 4, 5)))
            try:
                proxy.tambah(2**40, 1)
            except OverflowError as error:
                print("L1.3 integer > 2^31:", type(error).__name__, error)

            request_xml = xmlrpc.client.dumps((5, 3), methodname="tambah", allow_none=True)
            response_xml = xmlrpc.client.dumps((8,), methodresponse=True, allow_none=True)
            request_size = len(request_xml.encode("utf-8"))
            response_size = len(response_xml.encode("utf-8"))
            print(
                f"L1 ukuran request: {request_size} byte; payload 2 byte "
                f"({200/request_size:.2f}%)"
            )
            print(
                f"L1 ukuran response: {response_size} byte; payload 1 byte "
                f"({100/response_size:.2f}%)"
            )

            local_start = time.perf_counter()
            for _ in range(100):
                tambah(5, 3)
            local_time = time.perf_counter() - local_start
            rpc_start = time.perf_counter()
            for _ in range(100):
                proxy.tambah(5, 3)
            rpc_time = time.perf_counter() - rpc_start
            print(f"L1.4 100 pemanggilan lokal: {local_time:.6f} s")
            print(f"L1.4 100 pemanggilan XML-RPC localhost: {rpc_time:.6f} s")
            if local_time:
                print(f"Perbandingan waktu (RPC/lokal): {rpc_time/local_time:.1f}x")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    try:
        with ServerProxy(url, allow_none=True) as stopped_proxy:
            stopped_proxy.tambah(5, 3)
    except (OSError, xmlrpc.client.ProtocolError) as error:
        print(f"L1.5 server mati: {type(error).__name__}; client menangani koneksi gagal.")


if __name__ == "__main__":
    argparse.ArgumentParser(description="Pengukuran Praktikum 1").parse_args()
    run_experiments()
