from __future__ import annotations

import grpc

import legacy_akademik_pb2 as legacy_pb


class LegacyLayananNilaiStub:
    """Stub v1 yang tidak mengetahui field prodi pada respons versi baru."""

    def __init__(self, channel: grpc.Channel) -> None:
        self.GetMahasiswa = channel.unary_unary(
            "/akademik.LayananNilai/GetMahasiswa",
            request_serializer=legacy_pb.MahasiswaRequest.SerializeToString,
            response_deserializer=legacy_pb.Mahasiswa.FromString,
        )
