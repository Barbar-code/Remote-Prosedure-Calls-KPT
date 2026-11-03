# Praktikum 1 — RPC Dasar dengan XML-RPC

## Berkas

- `server_kalkulator.py` — server kalkulator XML-RPC, mencakup tambah, kali, bagi, pangkat, akar, statistik, dan echo datetime.
- `client_kalkulator.py` — client, termasuk `--verbose`, timeout, dan penanganan kegagalan koneksi.
- `experiments.py` — L1.1–L1.5: pangkat/akar/statistik, marshalling datetime dan integer besar, ukuran payload XML, benchmark lokal vs RPC, serta koneksi ke server yang dihentikan.

## Menjalankan

Instal dependensi bersama dari folder induk `Remote-Prosedure-Calls-KPT`:

```powershell
python -m pip install -r requirements.txt
```

Lalu, dari folder ini, jalankan server dan client di dua terminal:

```powershell
python server_kalkulator.py
python client_kalkulator.py --verbose
```

Untuk menjalankan latihan otomatis:

```powershell
python experiments.py
```

Ukuran body XML pada `tambah(5, 3)` yang diamati: request 190 byte (data 2 byte, 1,05%), response 121 byte (data 1 byte, 0,83%). Integer XML-RPC dibatasi signed 32-bit; `datetime` diubah menjadi XML-RPC `dateTime.iso8601`. Pengukuran jaringan dua komputer perlu dilakukan pada dua mesin yang dapat saling terhubung.
