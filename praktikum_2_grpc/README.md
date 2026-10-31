# Praktikum 2 — gRPC dan Protocol Buffers

## Berkas

- `akademik.proto` — kontrak RPC: unary, server-streaming, client-streaming, bidirectional-streaming, dan transfer.
- `akademik_pb2.py`, `akademik_pb2_grpc.py` — kode message dan stub hasil generate.
- `server_grpc.py`, `client_grpc.py` — layanan dan client gRPC.
- `legacy_akademik.proto`, file generated legacy, `legacy_client.py` — client v1 untuk uji backward compatibility setelah field `prodi = 4` ditambahkan.
- `experiments.py` — pengukuran ukuran Protobuf/XML dan chat dua arah untuk L2.2/L2.4.

## Menjalankan

Instal dependensi dari folder induk `rpc_practicum`:

```powershell
python -m pip install -r requirements.txt
```

Untuk regenerate stub setelah mengubah `.proto`, jalankan dari folder induk:

```powershell
python -m grpc_tools.protoc -I praktikum_2_grpc --python_out=praktikum_2_grpc --grpc_python_out=praktikum_2_grpc praktikum_2_grpc/akademik.proto praktikum_2_grpc/legacy_akademik.proto
```

Jalankan server dan client dari folder ini, masing-masing di terminal sendiri:

```powershell
python server_grpc.py
python client_grpc.py
```

Jalankan eksperimen L2 dari folder ini:

```powershell
python experiments.py all
```

`KirimBanyakNilai` mengimplementasikan client streaming; `Diskusi` meneruskan pesan antar-client yang terhubung. Client lama tetap dapat membaca field yang dikenalnya karena field baru memiliki nomor tersendiri. Untuk sampel mahasiswa pada laporan, serialisasi Protobuf adalah 24 byte dan XML-RPC 350 byte.
