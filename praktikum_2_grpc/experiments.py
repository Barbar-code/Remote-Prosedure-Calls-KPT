from __future__ import annotations

import concurrent.futures
import threading
import xmlrpc.client

import grpc

import akademik_pb2 as pb
import akademik_pb2_grpc as pb_grpc
from server_grpc import create_server


def run_protobuf_size() -> None:
    mahasiswa = pb.Mahasiswa(nim="2023001", nama="Andi", ipk=3.26)
    protobuf_bytes = mahasiswa.SerializeToString()
    xml = xmlrpc.client.dumps(
        ({"nim": mahasiswa.nim, "nama": mahasiswa.nama, "ipk": mahasiswa.ipk},),
        methodresponse=True,
        allow_none=True,
    ).encode("utf-8")
    print(f"L2.4 Protobuf: {len(protobuf_bytes)} byte")
    print(f"L2.4 XML-RPC response: {len(xml)} byte")
    print(f"XML-RPC/Protobuf: {len(xml)/len(protobuf_bytes):.1f}x")


def run_chat_demo() -> None:
    server, port, _ = create_server(port=0, slow_probability=0)
    server.start()
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    try:
        grpc.channel_ready_future(channel).result(timeout=5)
        stub = pb_grpc.LayananNilaiStub(channel)
        barrier = threading.Barrier(2)
        finished = {"A": threading.Event(), "B": threading.Event()}

        def send_messages(sender: str):
            barrier.wait(timeout=5)
            yield pb.Pesan(pengirim=sender, teks=f"pesan-1 dari {sender}")
            yield pb.Pesan(pengirim=sender, teks=f"pesan-2 dari {sender}")
            finished[sender].wait(timeout=5)

        def follow_chat(sender: str) -> list[pb.Pesan]:
            received = []
            for message in stub.Diskusi(send_messages(sender), timeout=7):
                received.append(message)
                if len(received) == 2:
                    finished[sender].set()
            return received

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            chat_a = pool.submit(follow_chat, "A")
            chat_b = pool.submit(follow_chat, "B")
            messages_a = chat_a.result(timeout=8)
            messages_b = chat_b.result(timeout=8)
        print("L2.2 pesan yang diterima A:", [(m.pengirim, m.teks) for m in messages_a])
        print("L2.2 pesan yang diterima B:", [(m.pengirim, m.teks) for m in messages_b])
    finally:
        channel.close()
        server.stop(grace=0).wait()


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Pengukuran Praktikum 2")
    parser.add_argument("latihan", choices=("protobuf-size", "chat", "all"))
    args = parser.parse_args()
    if args.latihan in ("protobuf-size", "all"):
        run_protobuf_size()
    if args.latihan in ("chat", "all"):
        run_chat_demo()


if __name__ == "__main__":
    main()
