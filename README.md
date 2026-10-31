# Praktikum RPC

Berkas sudah dipisahkan menurut praktikum supaya mudah ditemukan. Latihan teori pilihan ganda/esai dan Tugas Proyek Mini tidak dikerjakan, sesuai permintaan.

## Struktur folder

```text
rpc_practicum/
├── README.md
├── requirements.txt
├── praktikum_1_xmlrpc/
│   ├── README.md
│   ├── server_kalkulator.py
│   ├── client_kalkulator.py
│   └── experiments.py
├── praktikum_2_grpc/
│   ├── README.md
│   ├── akademik.proto
│   ├── akademik_pb2.py
│   ├── akademik_pb2_grpc.py
│   ├── legacy_akademik.proto
│   ├── legacy_akademik_pb2.py
│   ├── legacy_akademik_pb2_grpc.py
│   ├── legacy_client.py
│   ├── server_grpc.py
│   ├── client_grpc.py
│   └── experiments.py
├── praktikum_3_concurrency/
│   ├── README.md
│   └── experiments.py
└── tests/
    └── test_practicum.py
```

Praktikum 3 memakai kontrak dan server gRPC dari folder Praktikum 2, jadi berkas bersama itu tidak digandakan.

## Persiapan

Jalankan dari folder `rpc_practicum`:

```powershell
python -m pip install -r requirements.txt
python -m grpc_tools.protoc -I praktikum_2_grpc --python_out=praktikum_2_grpc --grpc_python_out=praktikum_2_grpc praktikum_2_grpc/akademik.proto praktikum_2_grpc/legacy_akademik.proto
```

Stub Protobuf hasil generate sudah disertakan; langkah generate hanya perlu dijalankan kembali setelah mengubah file `.proto`.

## Cara menjalankan

Semua perintah server dan client dijalankan dari folder praktikum masing-masing.

**Praktikum 1 — XML-RPC** (dua terminal, dari `praktikum_1_xmlrpc`):

```powershell
python server_kalkulator.py
python client_kalkulator.py --verbose
```

Jalankan pengukuran L1 dari folder yang sama:

```powershell
python experiments.py
```

**Praktikum 2 — gRPC/Protocol Buffers** (dua terminal, dari `praktikum_2_grpc`):

```powershell
python server_grpc.py
python client_grpc.py
```

Eksperimen L2 (ukuran Protobuf dan chat dua arah):

```powershell
python experiments.py all
```

**Praktikum 3 — concurrency, timeout, toleransi kegagalan** (dari `praktikum_3_concurrency`):

```powershell
python experiments.py all
```

Pilih eksperimen tertentu dengan `transfers`, `race`, `failover`, atau `benchmark` sebagai argumen.

## Ringkasan hasil dan analisis

- **L1:** 100 panggilan lokal dan RPC dibandingkan; XML request `tambah(5, 3)` berukuran 190 byte dengan payload angka 2 byte (1,05%), response 121 byte dengan payload 1 byte (0,83%). `datetime` dimarshal sebagai `dateTime.iso8601`; integer `2**40` gagal karena batas signed 32-bit XML-RPC. Server yang mati memunculkan `ConnectionRefusedError`/`OSError`.
- **L2:** client streaming `KirimBanyakNilai`, bidirectional streaming `Diskusi`, field kompatibel `prodi = 4`, dan 10 unary async calls sudah diimplementasikan. Sampel data mahasiswa yang sama: Protobuf 24 byte, XML-RPC 350 byte (14,6 kali lebih besar). Field Protobuf baru aman bagi pembaca lama karena nomor field lama tetap dan field tak dikenal diabaikan.
- **L3.1–L3.2:** 20 transfer menggunakan ID sama menghasilkan A=800, B=700. Dalam uji respons lambat, UUID baru setiap retry menyebabkan operasi ganda (saldo A=400, B=1100).
- **L3.3:** uji 50 transfer/10 thread tanpa lock menunjukkan saldo tidak konsisten karena race condition. Hasilnya dipengaruhi penjadwalan thread.
- **L3.4:** benchmark 200 permintaan/20 client thread, jeda server 0,1 s: 1 worker = 9,59 req/s (20,857 s; 1985,76 ms rata-rata), 4 = 37,42 req/s (5,344 s; 509,63 ms), 10 = 92,18 req/s (2,170 s; 206,48 ms). Hasil bergantung mesin dan beban.
- **L3.5:** failover mencoba endpoint berikutnya untuk `UNAVAILABLE` atau `DEADLINE_EXCEEDED`, tetapi tidak menutupi error aplikasi seperti `NOT_FOUND`. Deduplikasi dalam memori hilang saat restart; perbaikan produksi adalah menyimpan ID, hasil, dan mutasi saldo secara durable dalam satu transaksi atomik.

Perbandingan L1 dilakukan di localhost; pengukuran dengan dua komputer belum dilakukan. Data pengamatan sebelumnya dirangkum di folder praktikum masing-masing.

## Tes

Dari folder `rpc_practicum`:

```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```
