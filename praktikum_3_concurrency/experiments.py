from __future__ import annotations

import argparse
import concurrent.futures
import statistics
import sys
import time
import uuid
from pathlib import Path

import grpc

RPC_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = RPC_ROOT.parent
PRAKTIKUM_2 = RPC_ROOT / "praktikum_2_grpc"
sys.path.insert(0, str(WORKSPACE_ROOT))
sys.path.insert(0, str(PRAKTIKUM_2))

from rpc_practicum.praktikum_2_grpc import akademik_pb2 as pb
from rpc_practicum.praktikum_2_grpc import akademik_pb2_grpc as pb_grpc
from rpc_practicum.praktikum_2_grpc.client_grpc import (
    get_mahasiswa_failover,
    transfer_dengan_retry,
)
from rpc_practicum.praktikum_2_grpc.server_grpc import create_server


def run_benchmark() -> list[tuple[int, float, float, float]]:
    results = []
    for workers in (1, 4, 10):
        server, port, _ = create_server(
            host="127.0.0.1",
            port=0,
            max_workers=workers,
            service_delay=0.1,
            slow_probability=0,
        )
        server.start()
        channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        try:
            grpc.channel_ready_future(channel).result(timeout=5)
            stub = pb_grpc.LayananNilaiStub(channel)

            def call_once(_: int) -> float:
                start = time.perf_counter()
                response = stub.GetMahasiswa(pb.MahasiswaRequest(nim="2023001"), timeout=10)
                if not response.nama:
                    raise RuntimeError("Respons GetMahasiswa tidak valid")
                return time.perf_counter() - start

            start = time.perf_counter()
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
                latencies = list(pool.map(call_once, range(200)))
            duration = time.perf_counter() - start
            results.append((workers, duration, 200 / duration, statistics.mean(latencies) * 1000))
        finally:
            channel.close()
            server.stop(grace=0).wait()
    print("max_workers | waktu total (s) | throughput (req/s) | latensi rata-rata (ms)")
    for workers, duration, throughput, latency in results:
        print(f"{workers:11} | {duration:15.3f} | {throughput:18.2f} | {latency:23.2f}")
    return results


def run_transfer_demo() -> None:
    for unsafe_client_ids in (False, True):
        server, port, service = create_server(
            port=0,
            max_workers=20,
            slow_probability=1,
            slow_seconds=0.12,
        )
        server.start()
        channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        try:
            grpc.channel_ready_future(channel).result(timeout=5)
            stub = pb_grpc.LayananNilaiStub(channel)
            errors = 0
            for _ in range(20):
                try:
                    transfer_dengan_retry(
                        stub,
                        "A",
                        "B",
                        10,
                        maks=3,
                        timeout=0.03,
                        backoff_awal=0.005,
                        id_baru_setiap_percobaan=unsafe_client_ids,
                    )
                except grpc.RpcError:
                    errors += 1
            time.sleep(0.15)
            label = "UUID baru setiap retry" if unsafe_client_ids else "request_id sama (at-most-once)"
            print(f"L3.1/L3.2 {label}: saldo={service.balances}; timeout final={errors}")
        finally:
            channel.close()
            server.stop(grace=0).wait()


def run_race_demo() -> None:
    server, port, service = create_server(
        port=0,
        max_workers=10,
        slow_probability=0,
        unsafe_race_window=0.005,
    )
    server.start()
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    try:
        grpc.channel_ready_future(channel).result(timeout=5)
        stub = pb_grpc.LayananNilaiStub(channel)

        def send_transfer(_: int) -> None:
            response = stub.Transfer(
                pb.TransferRequest(
                    request_id=str(uuid.uuid4()),
                    dari="A",
                    ke="B",
                    jumlah=10,
                ),
                timeout=5,
            )
            if not response.sukses:
                raise RuntimeError(response.pesan)

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
            list(pool.map(send_transfer, range(50)))
        print(f"L3.3 tanpa lock: saldo={service.balances}; total={sum(service.balances.values()):.2f}")
    finally:
        channel.close()
        server.stop(grace=0).wait()


def run_failover_demo() -> None:
    unavailable, first_port, _ = create_server(port=0, slow_probability=0)
    healthy, second_port, _ = create_server(port=0, slow_probability=0)
    unavailable.start()
    unavailable.stop(grace=0).wait()
    healthy.start()
    first_channel = grpc.insecure_channel(f"127.0.0.1:{first_port}")
    second_channel = grpc.insecure_channel(f"127.0.0.1:{second_port}")
    try:
        grpc.channel_ready_future(second_channel).result(timeout=5)
        mahasiswa = get_mahasiswa_failover(
            [
                pb_grpc.LayananNilaiStub(first_channel),
                pb_grpc.LayananNilaiStub(second_channel),
            ],
            "2023001",
        )
        print(f"L3.5 failover berhasil: {mahasiswa.nama} melalui port {second_port}")
    finally:
        first_channel.close()
        second_channel.close()
        unavailable.stop(grace=0).wait()
        healthy.stop(grace=0).wait()


def main() -> None:
    parser = argparse.ArgumentParser(description="Eksperimen Praktikum 3")
    parser.add_argument("latihan", choices=("transfers", "race", "failover", "benchmark", "all"))
    args = parser.parse_args()
    routines = {
        "transfers": run_transfer_demo,
        "race": run_race_demo,
        "failover": run_failover_demo,
        "benchmark": run_benchmark,
    }
    if args.latihan == "all":
        for routine in routines.values():
            routine()
    else:
        routines[args.latihan]()


if __name__ == "__main__":
    main()
