# Praktični deo seminarskih radova – PostgreSQL

Svaka skripta se pokreće zasebno, sama pravi svoju bazu (ili izolovan klaster), izvršava
eksperiment korak po korak (koraci prate poglavlja rada) i na kraju generiše
**izveštaj (Markdown), CSV sa sirovim rezultatima i PNG grafike** za rad.

| # | Seminarski rad | Skripta | Uputstvo |
|---|---|---|---|
| 1 | Optimizacija upita primenom TPC-like benchmarking pristupa | [`seminar1_tpc_optimizacija/tpc_optimizacija.py`](seminar1_tpc_optimizacija/tpc_optimizacija.py) | [README](seminar1_tpc_optimizacija/README.md) |
| 2 | Full vs Incremental backup korišćenjem pgBackRest alata | [`seminar2_pgbackrest/pgbackrest_full_vs_incr.py`](seminar2_pgbackrest/pgbackrest_full_vs_incr.py) | [README](seminar2_pgbackrest/README.md) |
| 3 | OLTP i OLAP sistemi | [`seminar3_oltp_olap/oltp_olap_demo.py`](seminar3_oltp_olap/oltp_olap_demo.py) | [README](seminar3_oltp_olap/README.md) |

## Preduslovi

- Python 3.8+
- PostgreSQL 13+ (testirano na PostgreSQL 16)
- Python paketi:

  ```bash
  pip install -r requirements.txt
  ```
  (`psycopg2-binary` je obavezan; bez `matplotlib`-a skripte rade, ali ne prave grafike)


- Samo za seminarski 2: **pgBackRest** (na Windows-u koristiti **WSL**):
  ```bash
  wsl --list --verbose #proveri koje Linux distribucije imaš
  wsl --install -d Ubuntu #instaliraj Ubuntu
  wsl -d Ubuntu #pokreni Ubuntu
  wsl --set-default Ubuntu #postaviti Ubuntu kao podrazumevani
  sudo apt update
  sudo apt install postgresql pgbackrest
  pgbackrest version
  
  sudo apt install python3-pip
  cd root/path #gde ce venv biti
  sudo apt update
  sudo apt install python3-venv
  python3 -m venv .venv-wsl
  source .venv-wsl/bin/activate
  pip install psycopg2-binary
  ```

## Start

```bash
# Seminarski 1 i 3 – povezuju se na postojeći PostgreSQL server
python seminar1_tpc_optimizacija/tpc_optimizacija.py --user postgres --password postgres
python seminar3_oltp_olap/oltp_olap_demo.py          --user postgres --password postgres

# Seminarski 2 – pravi sopstveni privremeni klaster (pokrenuti kao običan korisnik, ne root), u WSL:
python3 seminar2_pgbackrest/pgbackrest_full_vs_incr.py --block-incr --lab ~/pgbr_lab
```

Rezultati se upisuju u `rezultati/` direktorijum pored svake skripte
(`izvestaj.md`, `*.csv`, `grafik_*.png`).
