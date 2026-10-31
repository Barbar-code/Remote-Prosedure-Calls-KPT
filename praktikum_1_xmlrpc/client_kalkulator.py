from __future__ import annotations

import argparse
import datetime
import http.client
import xmlrpc.client
from xmlrpc.client import ServerProxy, Transport


class TimeoutTransport(Transport):
    def __init__(self, timeout: float) -> None:
        super().__init__()
        self.timeout = timeout

    def make_connection(self, host: str) -> http.client.HTTPConnection:
        return http.client.HTTPConnection(host, timeout=self.timeout)


def main() -> None:
    parser = argparse.ArgumentParser(description="Client kalkulator XML-RPC")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--verbose", action="store_true", help="Tampilkan HTTP/XML request dan response")
    args = parser.parse_args()
    url = f"http://{args.host}:{args.port}/"

    try:
        with ServerProxy(
            url,
            allow_none=True,
            verbose=args.verbose,
            transport=TimeoutTransport(args.timeout),
        ) as proxy:
            print("Daftar method:", proxy.system.listMethods())
            print("5 + 3 =", proxy.tambah(5, 3))
            print("4 x 7 =", proxy.kali(4, 7))
            print("2 pangkat 10 =", proxy.pangkat(2, 10))
            print("akar(81) =", proxy.akar(81))
            print("statistik =", proxy.statistik([4, 7, 9, 12]))
            print("datetime =", proxy.echo_datetime(datetime.datetime(2025, 1, 2, 3, 4, 5)))

            try:
                proxy.bagi(10, 0)
            except xmlrpc.client.Fault as error:
                print("Fault dari server:", error.faultString)

            try:
                proxy.tambah(2**40, 1)
            except (OverflowError, TypeError) as error:
                print("Bilangan besar tidak dapat dikirim:", error)
    except (OSError, xmlrpc.client.ProtocolError) as error:
        print(f"RPC gagal ({url}): {type(error).__name__}: {error}")
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
