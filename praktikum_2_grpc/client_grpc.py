from __future__ import annotations

import argparse
import contextlib
import time
import uuid

import grpc

import akademik_pb2 as pb
import akademik_pb2_grpc as pb_grpc


def transfer_dengan_retry(
    stub: pb_grpc.LayananNilaiStub,
    dari: str,
    ke: str,
    jumlah: float,
    maks: int = 3,
    timeout: float = 2.0,
    backoff_awal: float = 0.5,
    id_baru_setiap_percobaan: bool = False,
) -> pb.StatusResponse:
    request_id = str(uuid.uuid4())
    for percobaan in range(1, maks + 1):
        if id_baru_setiap_percobaan:
            request_id = str(uuid.uuid4())
        request = pb.TransferRequest(
            request_id=request_id,
            dari=dari,
            ke=ke,
            jumlah=jumlah,
        )
        try:
            return stub.Transfer(request, timeout=timeout)
        except grpc.RpcError as error:
            boleh_diulang = error.code() in (
                grpc.StatusCode.DEADLINE_EXCEEDED,
                grpc.StatusCode.UNAVAILABLE,
            )
            if not boleh_diulang or percobaan == maks:
                raise
            jeda = backoff_awal * 2 ** (percobaan - 1)
            print(f"Percobaan {percobaan} gagal ({error.code().name}); ulang dalam {jeda:g} s")
            time.sleep(jeda)
    raise RuntimeError("Percobaan transfer berakhir tanpa respons")


def get_mahasiswa_failover(
    stubs: list[pb_grpc.LayananNilaiStub],
    nim: str,
) -> pb.Mahasiswa:
    for index, stub in enumerate(stubs):
        try:
            return stub.GetMahasiswa(pb.MahasiswaRequest(nim=nim), timeout=2.0)
        except grpc.RpcError as error:
            if error.code() not in (
                grpc.StatusCode.UNAVAILABLE,
                grpc.StatusCode.DEADLINE_EXCEEDED,
            ) or index == len(stubs) - 1:
                raise
            print(f"Server {index + 1} {error.code().name}; mencoba server berikutnya")
    raise RuntimeError("Tidak ada server failover yang dikonfigurasi")


def main() -> None:
    parser = argparse.ArgumentParser(description="Client gRPC layanan nilai")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--failover", help="Endpoint kedua dengan format host:port")
    args = parser.parse_args()

    endpoints = [f"{args.host}:{args.port}"]
    if args.failover:
        endpoints.append(args.failover)
    with contextlib.ExitStack() as stack:
        channels = [stack.enter_context(grpc.insecure_channel(endpoint)) for endpoint in endpoints]
        stubs = [pb_grpc.LayananNilaiStub(channel) for channel in channels]
        try:
            mahasiswa = get_mahasiswa_failover(stubs, "2023001")
            print(f"{mahasiswa.nama} ({mahasiswa.prodi}) - IPK {mahasiswa.ipk}")
            status = stubs[-1].TambahNilai(
                pb.NilaiRequest(nim="2023001", matkul="Komputasi Paralel", nilai=88),
                timeout=2.0,
            )
            print(status.pesan)
            for nilai in stubs[-1].StreamNilai(pb.MahasiswaRequest(nim="2023001"), timeout=2.0):
                print(f"  {nilai.matkul}: {nilai.nilai}")
            hasil_batch = stubs[-1].KirimBanyakNilai(
                iter(
                    [
                        pb.NilaiRequest(nim="2023002", matkul="Sistem Operasi", nilai=87),
                        pb.NilaiRequest(nim="2023002", matkul="Keamanan", nilai=90),
                    ]
                ),
                timeout=2.0,
            )
            print(hasil_batch.pesan)
            futures = [
                stubs[-1].GetMahasiswa.future(pb.MahasiswaRequest(nim="2023001"), timeout=2.0)
                for _ in range(10)
            ]
            print("Hasil 10 permintaan async:", [future.result().nama for future in futures])
        except grpc.RpcError as error:
            print(f"RPC gagal: {error.code().name}: {error.details()}")
            raise SystemExit(1) from error


if __name__ == "__main__":
    main()
