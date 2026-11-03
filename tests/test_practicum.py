from __future__ import annotations

import concurrent.futures
import datetime
import sys
import threading
import time
import unittest
import uuid
import xmlrpc.client
from pathlib import Path

RPC_ROOT = Path(__file__).resolve().parents[1]
PRAKTIKUM_2 = RPC_ROOT / "praktikum_2_grpc"
sys.path.insert(0, str(RPC_ROOT))
sys.path.insert(0, str(PRAKTIKUM_2))

import grpc

from praktikum_1_xmlrpc.server_kalkulator import akar, pangkat, statistik
from praktikum_2_grpc import akademik_pb2 as pb
from praktikum_2_grpc import akademik_pb2_grpc as pb_grpc
from praktikum_2_grpc import legacy_akademik_pb2 as legacy_pb
from praktikum_2_grpc.client_grpc import get_mahasiswa_failover
from praktikum_2_grpc.legacy_client import LegacyLayananNilaiStub
from praktikum_2_grpc.server_grpc import create_server


class XMLRPCPracticumTests(unittest.TestCase):
    def test_l1_math_and_statistics(self) -> None:
        self.assertEqual(pangkat(2, 10), 1024)
        self.assertEqual(akar(81), 9)
        self.assertEqual(statistik([4, 7, 9, 12]), {"min": 4, "max": 12, "rata2": 8})
        with self.assertRaises(ValueError):
            statistik([])

    def test_l1_datetime_and_integer_marshalling(self) -> None:
        date_xml = xmlrpc.client.dumps((datetime.datetime(2025, 1, 2),))
        self.assertIn("dateTime.iso8601", date_xml)
        with self.assertRaises(OverflowError):
            xmlrpc.client.dumps((2**40,))


class GRPCPracticumTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.server, port, cls.service = create_server(
            host="127.0.0.1",
            port=0,
            max_workers=10,
            slow_probability=0,
        )
        cls.server.start()
        cls.channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        grpc.channel_ready_future(cls.channel).result(timeout=5)
        cls.stub = pb_grpc.LayananNilaiStub(cls.channel)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.channel.close()
        cls.server.stop(grace=0).wait()

    def test_l2_unary_streaming_and_client_streaming(self) -> None:
        mahasiswa = self.stub.GetMahasiswa(pb.MahasiswaRequest(nim="247006111191"))
        self.assertEqual(mahasiswa.nama, "Muhammad Kibar Ramadhan")
        self.assertEqual(mahasiswa.prodi, "Informatika")

        nilai = list(self.stub.StreamNilai(pb.MahasiswaRequest(nim="247006111191")))
        self.assertEqual(
            [item.matkul for item in nilai],
            ["Basis Data", "Jaringan Komputer", "Komputasi Parallel Terdistribusi"],
        )

        result = self.stub.KirimBanyakNilai(
            iter(
                [
                    pb.NilaiRequest(nim="247006111191", matkul="Basis Data", nilai=87),
                    pb.NilaiRequest(nim="247006111191", matkul="Jaringan Komputer", nilai=90),
                ]
            )
        )
        self.assertEqual(result.pesan, "2 nilai tersimpan")

    def test_l2_bidirectional_chat(self) -> None:
        mulai_kirim = threading.Event()
        selesai = {"A": threading.Event(), "B": threading.Event()}

        def kirim(pengirim: str):
            if not mulai_kirim.wait(timeout=5):
                raise TimeoutError("Client chat belum siap mengirim")
            yield pb.Pesan(pengirim=pengirim, teks=f"halo-{pengirim}")
            selesai[pengirim].wait(timeout=5)

        def ikuti(pengirim: str):
            messages = []
            for message in self.stub.Diskusi(kirim(pengirim), timeout=7):
                messages.append(message)
                selesai[pengirim].set()
            return messages

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            hasil_a = pool.submit(ikuti, "A")
            hasil_b = pool.submit(ikuti, "B")
            batas_waktu = time.monotonic() + 5
            while time.monotonic() < batas_waktu:
                with self.service.chat_lock:
                    if len(self.service.chat_clients) == 2:
                        break
                time.sleep(0.01)
            else:
                self.fail("Kedua client chat tidak terdaftar pada server")
            mulai_kirim.set()
            self.assertEqual([(p.pengirim, p.teks) for p in hasil_a.result(timeout=8)], [("B", "halo-B")])
            self.assertEqual([(p.pengirim, p.teks) for p in hasil_b.result(timeout=8)], [("A", "halo-A")])

    def test_l2_async_unary_calls(self) -> None:
        futures = [
            self.stub.GetMahasiswa.future(pb.MahasiswaRequest(nim="247006111191"))
            for _ in range(10)
        ]
        self.assertEqual(
            [future.result(timeout=5).nama for future in futures],
            ["Muhammad Kibar Ramadhan"] * 10,
        )

    def test_l3_failover_from_unavailable_server(self) -> None:
        stopped_server, port, _ = create_server(host="127.0.0.1", port=0, slow_probability=0)
        stopped_server.start()
        stopped_server.stop(grace=0).wait()
        failed_channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        try:
            mahasiswa = get_mahasiswa_failover(
                [pb_grpc.LayananNilaiStub(failed_channel), self.stub],
                "247006111191",
            )
            self.assertEqual(mahasiswa.nama, "Muhammad Kibar Ramadhan")
        finally:
            failed_channel.close()

    def test_l2_validation_and_unknown_field_compatibility(self) -> None:
        with self.assertRaises(grpc.RpcError) as error:
            self.stub.GetMahasiswa(pb.MahasiswaRequest(nim="not-found"))
        self.assertEqual(error.exception.code(), grpc.StatusCode.NOT_FOUND)

        current = pb.Mahasiswa(
            nim="247006111191",
            nama="Muhammad Kibar Ramadhan",
            ipk=3.35,
            prodi="Informatika",
        )
        old_reader = legacy_pb.Mahasiswa.FromString(current.SerializeToString())
        self.assertEqual(old_reader.nim, current.nim)
        self.assertEqual(old_reader.nama, current.nama)
        old_stub = LegacyLayananNilaiStub(self.channel)
        old_response = old_stub.GetMahasiswa(legacy_pb.MahasiswaRequest(nim="247006111191"))
        self.assertEqual(old_response.nama, "Muhammad Kibar Ramadhan")

    def test_l3_transfer_deduplication(self) -> None:
        request = pb.TransferRequest(
            request_id=str(uuid.uuid4()),
            dari="A",
            ke="B",
            jumlah=10,
        )
        first = self.stub.Transfer(request)
        second = self.stub.Transfer(request)
        self.assertTrue(first.sukses)
        self.assertEqual(first, second)
        self.assertEqual(self.service.balances, {"A": 990.0, "B": 510.0})


if __name__ == "__main__":
    unittest.main()
