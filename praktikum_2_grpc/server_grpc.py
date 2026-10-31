from __future__ import annotations

import argparse
import math
import queue
import random
import threading
import time
from concurrent import futures
from dataclasses import dataclass

import grpc

import akademik_pb2 as pb
import akademik_pb2_grpc as pb_grpc


@dataclass(frozen=True)
class CachedTransfer:
    signature: tuple[str, str, float]
    response: pb.StatusResponse


DATA: dict[str, dict[str, object]] = {
    "2023001": {
        "nama": "Andi",
        "prodi": "Informatika",
        "nilai": {"Basis Data": 85.0, "Jaringan": 78.0},
    },
    "2023002": {
        "nama": "Sari",
        "prodi": "Sistem Informasi",
        "nilai": {"Basis Data": 92.0},
    },
}


class LayananNilai(pb_grpc.LayananNilaiServicer):
    def __init__(
        self,
        service_delay: float = 0.0,
        slow_probability: float = 0.3,
        slow_seconds: float = 3.0,
        unsafe_race_window: float = 0.0,
        seed: int | None = None,
    ) -> None:
        self.data = {
            nim: {
                "nama": str(mhs["nama"]),
                "prodi": str(mhs["prodi"]),
                "nilai": dict(mhs["nilai"]),
            }
            for nim, mhs in DATA.items()
        }
        self.balances = {"A": 1000.0, "B": 500.0}
        self.processed: dict[str, CachedTransfer] = {}
        self.inflight: dict[str, tuple[tuple[str, str, float], threading.Event]] = {}
        self.lock = threading.Lock()
        self.chat_lock = threading.Lock()
        self.chat_clients: set[queue.Queue[pb.Pesan | None]] = set()
        self.service_delay = service_delay
        self.slow_probability = slow_probability
        self.slow_seconds = slow_seconds
        self.unsafe_race_window = unsafe_race_window
        self.random = random.Random(seed)

    def GetMahasiswa(self, request: pb.MahasiswaRequest, context: grpc.ServicerContext) -> pb.Mahasiswa:
        if self.service_delay:
            time.sleep(self.service_delay)
        with self.lock:
            mahasiswa = self.data.get(request.nim)
            if mahasiswa is None:
                context.abort(grpc.StatusCode.NOT_FOUND, f"NIM {request.nim} tidak ditemukan")
            nilai = list(mahasiswa["nilai"].values())
            ipk = sum(nilai) / len(nilai) / 25 if nilai else 0.0
            return pb.Mahasiswa(
                nim=request.nim,
                nama=str(mahasiswa["nama"]),
                ipk=round(ipk, 2),
                prodi=str(mahasiswa["prodi"]),
            )

    def TambahNilai(self, request: pb.NilaiRequest, context: grpc.ServicerContext) -> pb.StatusResponse:
        if not request.nim or not request.matkul:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "NIM dan mata kuliah wajib diisi")
        if not math.isfinite(request.nilai) or not 0 <= request.nilai <= 100:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Nilai harus 0–100")
        with self.lock:
            mahasiswa = self.data.setdefault(
                request.nim,
                {"nama": "(baru)", "prodi": "(belum diisi)", "nilai": {}},
            )
            mahasiswa["nilai"][request.matkul] = request.nilai
        return pb.StatusResponse(sukses=True, pesan="Nilai tersimpan")

    def StreamNilai(self, request: pb.MahasiswaRequest, context: grpc.ServicerContext):
        with self.lock:
            mahasiswa = self.data.get(request.nim)
            if mahasiswa is None:
                context.abort(grpc.StatusCode.NOT_FOUND, f"NIM {request.nim} tidak ditemukan")
            nilai = list(mahasiswa["nilai"].items())
        for matkul, angka in nilai:
            if not context.is_active():
                return
            yield pb.Nilai(matkul=matkul, nilai=angka)
            time.sleep(0.05)

    def KirimBanyakNilai(self, request_iterator, context: grpc.ServicerContext) -> pb.StatusResponse:
        tersimpan = 0
        for request in request_iterator:
            if not request.nim or not request.matkul:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "NIM dan mata kuliah wajib diisi")
            if not math.isfinite(request.nilai) or not 0 <= request.nilai <= 100:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Nilai harus 0–100")
            with self.lock:
                mahasiswa = self.data.setdefault(
                    request.nim,
                    {"nama": "(baru)", "prodi": "(belum diisi)", "nilai": {}},
                )
                mahasiswa["nilai"][request.matkul] = request.nilai
            tersimpan += 1
        return pb.StatusResponse(sukses=True, pesan=f"{tersimpan} nilai tersimpan")

    def Diskusi(self, request_iterator, context: grpc.ServicerContext):
        inbox: queue.Queue[pb.Pesan | None] = queue.Queue()
        with self.chat_lock:
            self.chat_clients.add(inbox)

        def terima_pesan() -> None:
            try:
                for pesan in request_iterator:
                    with self.chat_lock:
                        penerima = tuple(client for client in self.chat_clients if client is not inbox)
                    for client in penerima:
                        client.put(pesan)
            finally:
                inbox.put(None)

        threading.Thread(target=terima_pesan, daemon=True).start()
        try:
            while context.is_active():
                try:
                    pesan = inbox.get(timeout=0.2)
                except queue.Empty:
                    continue
                if pesan is None:
                    return
                yield pesan
        finally:
            with self.chat_lock:
                self.chat_clients.discard(inbox)

    def Transfer(self, request: pb.TransferRequest, context: grpc.ServicerContext) -> pb.StatusResponse:
        signature = (request.dari, request.ke, request.jumlah)
        if not request.request_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "request_id wajib diisi")
        if request.dari == request.ke or not math.isfinite(request.jumlah) or request.jumlah <= 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Transfer tidak valid")

        while True:
            with self.lock:
                cached = self.processed.get(request.request_id)
                if cached is not None:
                    if cached.signature != signature:
                        context.abort(
                            grpc.StatusCode.INVALID_ARGUMENT,
                            "request_id yang sama tidak boleh dipakai untuk isi berbeda",
                        )
                    return cached.response
                existing = self.inflight.get(request.request_id)
                if existing is None:
                    selesai = threading.Event()
                    self.inflight[request.request_id] = (signature, selesai)
                    break
                if existing[0] != signature:
                    context.abort(
                        grpc.StatusCode.INVALID_ARGUMENT,
                        "request_id yang sama tidak boleh dipakai untuk isi berbeda",
                    )
                selesai = existing[1]
            selesai.wait()

        try:
            if self.unsafe_race_window:
                asal = self.balances.get(request.dari)
                tujuan = self.balances.get(request.ke)
                if asal is None or tujuan is None:
                    context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Akun tidak dikenal")
                if asal < request.jumlah:
                    response = pb.StatusResponse(sukses=False, pesan="Saldo tidak cukup")
                else:
                    self.balances[request.dari] = asal - request.jumlah
                    time.sleep(self.unsafe_race_window)
                    self.balances[request.ke] = tujuan + request.jumlah
                    response = pb.StatusResponse(sukses=True, pesan="Transfer tersimpan")
            else:
                with self.lock:
                    asal = self.balances.get(request.dari)
                    tujuan = self.balances.get(request.ke)
                    if asal is None or tujuan is None:
                        context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Akun tidak dikenal")
                    if asal < request.jumlah:
                        response = pb.StatusResponse(sukses=False, pesan="Saldo tidak cukup")
                    else:
                        self.balances[request.dari] = asal - request.jumlah
                        self.balances[request.ke] = tujuan + request.jumlah
                        response = pb.StatusResponse(sukses=True, pesan="Transfer tersimpan")

            with self.lock:
                self.processed[request.request_id] = CachedTransfer(signature, response)
                _, selesai = self.inflight.pop(request.request_id)
                selesai.set()
        except Exception:
            with self.lock:
                pending = self.inflight.pop(request.request_id, None)
                if pending is not None:
                    pending[1].set()
            raise

        if self.random.random() < self.slow_probability:
            time.sleep(self.slow_seconds)
        return response


def create_server(
    host: str = "127.0.0.1",
    port: int = 50051,
    max_workers: int = 10,
    service_delay: float = 0.0,
    slow_probability: float = 0.3,
    slow_seconds: float = 3.0,
    unsafe_race_window: float = 0.0,
    seed: int | None = None,
) -> tuple[grpc.Server, int, LayananNilai]:
    service = LayananNilai(
        service_delay=service_delay,
        slow_probability=slow_probability,
        slow_seconds=slow_seconds,
        unsafe_race_window=unsafe_race_window,
        seed=seed,
    )
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    pb_grpc.add_LayananNilaiServicer_to_server(service, server)
    bound_port = server.add_insecure_port(f"{host}:{port}")
    if bound_port == 0:
        raise OSError(f"Gagal membuka port {host}:{port}")
    return server, bound_port, service


def main() -> None:
    parser = argparse.ArgumentParser(description="Server layanan nilai gRPC")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--max-workers", type=int, default=10)
    parser.add_argument("--service-delay", type=float, default=0.0)
    parser.add_argument("--slow-probability", type=float, default=0.3)
    parser.add_argument("--slow-seconds", type=float, default=3.0)
    parser.add_argument("--unsafe-race-window", type=float, default=0.0)
    parser.add_argument("--seed", type=int)
    args = parser.parse_args()
    if args.max_workers < 1:
        parser.error("--max-workers harus lebih besar dari 0")
    if not 0 <= args.slow_probability <= 1:
        parser.error("--slow-probability harus antara 0 dan 1")
    if min(args.service_delay, args.slow_seconds, args.unsafe_race_window) < 0:
        parser.error("Parameter waktu tidak boleh negatif")

    server, port, _ = create_server(
        args.host,
        args.port,
        args.max_workers,
        args.service_delay,
        args.slow_probability,
        args.slow_seconds,
        args.unsafe_race_window,
        args.seed,
    )
    server.start()
    print(f"Server gRPC berjalan di {args.host}:{port} (max_workers={args.max_workers})...")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("\nServer dihentikan.")
    finally:
        server.stop(grace=1).wait()


if __name__ == "__main__":
    main()
