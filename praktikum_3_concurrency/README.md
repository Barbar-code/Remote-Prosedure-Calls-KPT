# Praktikum 3 — Concurrency, Timeout, dan Toleransi Kegagalan

## Berkas dan dependensi

- `experiments.py` — eksperimen L3.1–L3.5 untuk retry/deduplikasi, race condition, failover, dan throughput.
- `hasil_benchmark.png` — grafik throughput dan latensi rata-rata, otomatis dibuat saat benchmark dijalankan.
- Kontrak, generated stub, serta server/client gRPC dipakai bersama dari folder `praktikum_2_grpc`; tidak disalin agar implementasinya tetap konsisten.

Instal dependensi dari folder induk `Remote-Prosedure-Calls-KPT`:

```powershell
python -m pip install -r requirements.txt
```

Jalankan dari folder ini:

```powershell
python experiments.py all
```

Perintah `all` menjalankan benchmark dan menyimpan grafik throughput serta latensi sebagai `hasil_benchmark.png` di folder ini. Perintah `python experiments.py benchmark` juga membuat grafik yang sama.

Atau pilih `transfers`, `race`, `failover`, atau `benchmark` untuk menjalankan satu latihan.

## Ringkasan pengamatan

- ID permintaan yang tetap selama retry mencegah transfer yang sama diterapkan dua kali; ID baru pada retry memungkinkan duplikasi.
- Uji tanpa lock menunjukkan bahwa update saldo bersamaan dapat kehilangan perubahan; saldo dan totalnya menjadi tidak konsisten.
- Failover berpindah saat endpoint memberi `UNAVAILABLE` atau `DEADLINE_EXCEEDED`. Error aplikasi tetap diteruskan ke pemanggil.
- Throughput benchmark (200 RPC, 20 client thread, jeda server 0,1 s) berubah sesuai jumlah worker dan kapasitas mesin. Catat ulang hasil eksperimen pada mesin yang digunakan.
- Cache deduplikasi saat ini hanya hidup di memori. Setelah server restart, jaminan deduplikasi hilang; sistem nyata perlu menyimpan ID permintaan dan transaksi saldo secara durable dan atomik.
