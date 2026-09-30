#!/usr/bin/env python3
"""
Seminarski rad: Full vs Incremental backup korišćenjem pgBackRest alata
=======================================================================

Skripta u izolovanoj "laboratoriji" (poseban PostgreSQL klaster koji sama
kreira, ne dira postojeće baze) demonstrira:

  KORAK 1  Provera preduslova (pgBackRest, PostgreSQL binarni fajlovi, korisnik)
  KORAK 2  Kreiranje laboratorijskog klastera (initdb, data checksums)
  KORAK 3  Konfiguracija WAL arhiviranja i pgBackRest-a (stanza-create, check)
  KORAK 4  Punjenje test podataka (big_static, hot_table, event_log)
  KORAK 5  FULL backup
  KORAK 6  INCR #1  - posle malog INSERT-a u event_log
  KORAK 7  INCR #2  - posle UPDATE-a 1% redova u hot_table (granularnost = fajl!)
  KORAK 8  Restore INCR #2 u novi direktorijum + verifikacija (lanac FULL->INCR1->INCR2)
  KORAK 9  DIFF backup i INCR #3 (zavisnosti differential backupa)
  KORAK 10 Point-in-time recovery (restore point -> DROP TABLE -> restore)
  KORAK 11 Politika zadržavanja (retention) - expire briše ceo lanac
  KORAK 12 Zbirni rezultati, grafici i izveštaj

Opcija --block-incr uključuje drugi repozitorijum (repo2) sa block
incremental backup-om (pgBackRest >= 2.46) - isti backup-i se prave u oba
repozitorijuma pa se direktno poredi file-level i block-level inkrement.

Zahtevi: Linux/macOS (na Windows-u koristiti WSL), pgBackRest, PostgreSQL
server (initdb, pg_ctl), psycopg2. Pokretati kao OBIČAN korisnik (ne root).

Pokretanje (primer):
    python3 pgbackrest_full_vs_incr.py --scale 500 --block-incr
"""

import argparse
import csv
import getpass
import glob
import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    import psycopg2
except ImportError:
    sys.exit("Nedostaje paket psycopg2. Instalirajte ga: pip install psycopg2-binary")

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    HAVE_MPL = True
except ImportError:
    HAVE_MPL = False

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(errors="replace")

STANZA = "demo"
LAB_MARKER = ".pgbr_lab"


# ---------------------------------------------------------------------------
# Pomoćne funkcije za ispis
# ---------------------------------------------------------------------------

def korak(broj, naslov):
    print()
    print("=" * 78)
    print(f"=== KORAK {broj}: {naslov}")
    print("=" * 78)


def info(tekst=""):
    print(f"    {tekst}")


def mb(n):
    return f"{(n or 0) / 1024 / 1024:.1f} MB"


def print_table(headers, rows):
    widths = [len(str(h)) for h in headers]
    for r in rows:
        for i, v in enumerate(r):
            widths[i] = max(widths[i], len(str(v)))
    info("  ".join(str(h).ljust(widths[i]) for i, h in enumerate(headers)))
    info("  ".join("-" * w for w in widths))
    for r in rows:
        info("  ".join(str(v).ljust(widths[i]) for i, v in enumerate(r)))


def md_table(headers, rows):
    out = ["| " + " | ".join(str(h) for h in headers) + " |",
           "|" + "|".join("---" for _ in headers) + "|"]
    for r in rows:
        out.append("| " + " | ".join(str(v) for v in r) + " |")
    return "\n".join(out)


# ---------------------------------------------------------------------------
# Laboratorija: putanje, pokretanje komandi, klaster
# ---------------------------------------------------------------------------

class Lab:
    def __init__(self, root: Path, port: int, pg_bin: Path, compress: str, block_incr: bool):
        self.root = root
        self.data = root / "data"
        self.restore_data = root / "restore_data"
        self.socket = root / "socket"
        self.socket_restore = root / "socket_restore"
        self.repo1 = root / "repo1"
        self.repo2 = root / "repo2"
        self.log = root / "log"
        self.lock = root / "lock"
        self.spool = root / "spool"
        self.conf = root / "pgbackrest.conf"
        self.port = port
        self.restore_port = port + 1
        self.pg_bin = pg_bin
        self.compress = compress
        self.block_incr = block_incr
        self.user = getpass.getuser()

    def bin(self, name):
        return str(self.pg_bin / name)

    def repos(self):
        return [1, 2] if self.block_incr else [1]


def run(cmd, check=True, quiet=False, show_filter=None):
    """Pokreće spoljnu komandu; vraća (trajanje_s, stdout)."""
    t0 = time.perf_counter()
    p = subprocess.run(cmd, capture_output=True, text=True)
    dt = time.perf_counter() - t0
    out = (p.stdout or "") + (p.stderr or "")
    if check and p.returncode != 0:
        print(out)
        sys.exit(f"Komanda nije uspela ({p.returncode}): {' '.join(cmd)}")
    if not quiet and show_filter:
        for line in out.splitlines():
            if re.search(show_filter, line):
                # skraćujemo vremensku oznaku i PID na početku linije
                info("  | " + re.sub(r"^\S+ \S+ P\d+\s+", "", line))
    return dt, out


def pgbr(lab, *args, **kw):
    return run(["pgbackrest", f"--config={lab.conf}", f"--stanza={STANZA}", *args], **kw)


def connect(lab, restore=False):
    conn = psycopg2.connect(host=str(lab.socket_restore if restore else lab.socket),
                            port=lab.restore_port if restore else lab.port,
                            user=lab.user, dbname="postgres")
    conn.autocommit = True
    return conn


def sql(conn, query, params=None, fetch=False):
    with conn.cursor() as cur:
        cur.execute(query, params)
        if fetch:
            return cur.fetchall()
    return None


def pg_start(lab, data_dir, logfile, extra_opts=None):
    cmd = [lab.bin("pg_ctl"), "-D", str(data_dir), "-l", str(logfile), "-w", "-t", "120"]
    if extra_opts:
        cmd += ["-o", extra_opts]
    return run(cmd + ["start"])[0]


def pg_stop(lab, data_dir, check=True):
    return run([lab.bin("pg_ctl"), "-D", str(data_dir), "-m", "fast", "-w", "stop"], check=check, quiet=True)


def pg_running(lab, data_dir):
    if not (Path(data_dir) / "postmaster.pid").exists():
        return False
    p = subprocess.run([lab.bin("pg_ctl"), "-D", str(data_dir), "status"], capture_output=True)
    return p.returncode == 0


# ---------------------------------------------------------------------------
# Provera preduslova
# ---------------------------------------------------------------------------

def find_pg_bin(user_path):
    if user_path:
        return Path(user_path)
    if shutil.which("pg_ctl") and shutil.which("initdb"):
        return Path(shutil.which("pg_ctl")).parent
    candidates = (glob.glob("/usr/lib/postgresql/*/bin") + glob.glob("/usr/pgsql-*/bin") +
                  glob.glob("/opt/homebrew/opt/postgresql@*/bin") + glob.glob("/usr/local/opt/postgresql@*/bin") +
                  glob.glob("/usr/local/pgsql/bin"))
    candidates = [c for c in candidates if os.path.exists(os.path.join(c, "initdb"))]

    def ver(c):
        m = re.search(r"(\d+)", c)
        return int(m.group(1)) if m else 0
    if not candidates:
        return None
    return Path(sorted(candidates, key=ver)[-1])


# ---------------------------------------------------------------------------
# Test podaci i kontrolne sume
# ---------------------------------------------------------------------------

TABLES = ["big_static", "hot_table", "event_log"]


def table_fingerprint(conn):
    """Broj redova i md5 kontrolna suma sadržaja svake tabele."""
    out = {}
    for t in TABLES:
        rows = sql(conn, "SELECT to_regclass(%s) IS NOT NULL", (t,), fetch=True)
        if not rows[0][0]:
            out[t] = None
            continue
        cnt, digest = sql(conn, f"SELECT count(*), md5(string_agg(md5(x::text), '' ORDER BY id)) FROM {t} x",
                          fetch=True)[0]
        out[t] = (cnt, digest)
    return out


def fingerprint_str(fp):
    return ", ".join(f"{t}={v[0]:,} redova/{v[1][:8]}" if v else f"{t}=NE POSTOJI" for t, v in fp.items())


def db_size(conn):
    return sql(conn, "SELECT pg_database_size('postgres')", fetch=True)[0][0]


def relation_files(conn, table):
    path, size = sql(conn, "SELECT pg_relation_filepath(%s), pg_relation_size(%s)", (table, table), fetch=True)[0]
    return path, size


# ---------------------------------------------------------------------------
# pgBackRest info (JSON)
# ---------------------------------------------------------------------------

def backup_info(lab, repo=None):
    args = ["--output=json"]
    if repo:
        args.append(f"--repo={repo}")
    _, out = pgbr(lab, *args, "info", quiet=True)
    data = json.loads(out)
    return data[0].get("backup", []) if data else []


def latest_backup(lab, repo):
    backups = [b for b in backup_info(lab, repo) if b.get("database", {}).get("repo-key", repo) == repo]
    return backups[-1] if backups else None


# ---------------------------------------------------------------------------
# Glavni tok
# ---------------------------------------------------------------------------

def parse_args():
    here = Path(__file__).resolve().parent
    p = argparse.ArgumentParser(description="Full vs Incremental backup sa pgBackRest-om (izolovana laboratorija)")
    p.add_argument("--lab", default=str(here / "pgbr_lab"), help="direktorijum laboratorije")
    p.add_argument("--port", type=int, default=5433, help="port laboratorijskog klastera (restore koristi port+1)")
    p.add_argument("--scale", type=int, default=500, help="približna veličina test podataka u MB (default 500)")
    p.add_argument("--compress", default="lz4", choices=["lz4", "zst", "gz", "bz2", "none"],
                   help="algoritam kompresije u repozitorijumu")
    p.add_argument("--block-incr", action="store_true",
                   help="dodatni repo2 sa block incremental backup-om (poređenje sa file-level)")
    p.add_argument("--pg-bin", default=None, help="direktorijum sa initdb/pg_ctl (auto-detekcija ako se izostavi)")
    p.add_argument("--out", default=str(here / "rezultati"), help="direktorijum za rezultate")
    p.add_argument("--keep-running", action="store_true", help="ostaviti laboratorijski klaster pokrenut na kraju")
    p.add_argument("--cleanup", action="store_true", help="na kraju obrisati ceo direktorijum laboratorije")
    return p.parse_args()


def main():
    args = parse_args()
    started = datetime.now()
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ 1
    korak(1, "Provera preduslova")
    if os.name != "posix" or not hasattr(os, "geteuid"):
        sys.exit("pgBackRest radi samo na Linux/Unix sistemima. Na Windows-u pokrenite skriptu u WSL-u.")
    if os.geteuid() == 0:
        sys.exit("PostgreSQL ne sme da se pokreće kao root. Pokrenite skriptu kao običan korisnik,\n"
                 "npr.: sudo -u postgres python3 pgbackrest_full_vs_incr.py --lab /tmp/pgbr_lab")
    if not shutil.which("pgbackrest"):
        sys.exit("pgBackRest nije instaliran. Ubuntu/Debian: sudo apt install pgbackrest; "
                 "RHEL: sudo dnf install pgbackrest; macOS: brew install pgbackrest")
    pg_bin = find_pg_bin(args.pg_bin)
    if not pg_bin:
        sys.exit("Nisu pronađeni initdb/pg_ctl. Navedite putanju opcijom --pg-bin.")
    _, v = run(["pgbackrest", "version"], quiet=True)
    pgbr_version = v.strip()
    _, v = run([str(pg_bin / "postgres"), "--version"], quiet=True)
    pg_version = v.strip()
    info(f"pgBackRest : {pgbr_version}")
    info(f"PostgreSQL : {pg_version} ({pg_bin})")
    info(f"Korisnik   : {getpass.getuser()}")
    m = re.search(r"(\d+)\.(\d+)", pgbr_version)
    if args.block_incr and m and (int(m.group(1)), int(m.group(2))) < (2, 46):
        info("UPOZORENJE: block incremental zahteva pgBackRest >= 2.46 - opcija se isključuje.")
        args.block_incr = False

    lab = Lab(Path(args.lab).resolve(), args.port, pg_bin, args.compress, args.block_incr)

    # ------------------------------------------------------------------ 2
    korak(2, "Kreiranje laboratorijskog PostgreSQL klastera")
    if lab.root.exists():
        if not (lab.root / LAB_MARKER).exists():
            sys.exit(f"Direktorijum {lab.root} postoji, a nije laboratorija ove skripte - neću ga obrisati.")
        for d in (lab.data, lab.restore_data):
            if pg_running(lab, d):
                pg_stop(lab, d, check=False)
        shutil.rmtree(lab.root)
        info("Obrisana prethodna laboratorija.")
    for d in (lab.root, lab.socket, lab.socket_restore, lab.repo1, lab.log, lab.lock, lab.spool):
        d.mkdir(parents=True, exist_ok=True)
    if lab.block_incr:
        lab.repo2.mkdir(exist_ok=True)
    (lab.root / LAB_MARKER).write_text("pgbackrest lab\n")
    os.chmod(lab.socket, 0o700)
    os.chmod(lab.socket_restore, 0o700)

    dt, _ = run([lab.bin("initdb"), "-D", str(lab.data), "--auth=trust", "--data-checksums",
                 "--encoding=UTF8", "--locale=C"], quiet=True)
    info(f"initdb (sa --data-checksums) završen za {dt:.1f} s: {lab.data}")

    # ------------------------------------------------------------------ 3
    korak(3, "Konfiguracija WAL arhiviranja i pgBackRest-a")
    archive_cmd = f'pgbackrest --config="{lab.conf}" --stanza={STANZA} archive-push %p'
    pg_conf = f"""
# ---- Podešavanja laboratorije (dodala skripta) ----
port = {lab.port}
listen_addresses = ''                 # samo unix socket, bez TCP-a
unix_socket_directories = '{lab.socket}'
wal_level = replica                   # dovoljno informacija u WAL-u za backup/PITR
archive_mode = on                     # uključeno arhiviranje WAL segmenata
archive_command = '{archive_cmd}'
archive_timeout = 60
max_wal_senders = 3
shared_buffers = 128MB
max_wal_size = 1GB
autovacuum = off                      # SAMO radi determinističkog eksperimenta!
"""
    with open(lab.data / "postgresql.conf", "a", encoding="utf-8") as f:
        f.write(pg_conf)
    info("postgresql.conf: wal_level=replica, archive_mode=on, archive_command=pgbackrest archive-push")

    repo2 = ""
    if lab.block_incr:
        repo2 = f"""
repo2-path={lab.repo2}
repo2-retention-full=2
repo2-bundle=y
repo2-block=y
"""
    compress = "none" if lab.compress == "none" else lab.compress
    lab.conf.write_text(f"""[global]
repo1-path={lab.repo1}
repo1-retention-full=2
{repo2.strip()}
compress-type={compress}
start-fast=y
log-path={lab.log}
lock-path={lab.lock}
spool-path={lab.spool}
log-level-console=info
log-level-file=detail

[{STANZA}]
pg1-path={lab.data}
pg1-port={lab.port}
pg1-socket-path={lab.socket}
""", encoding="utf-8")
    info(f"pgbackrest.conf: {lab.conf}")
    for line in lab.conf.read_text().splitlines():
        if line.strip():
            info("  | " + line)

    pg_start(lab, lab.data, lab.log / "postgres.log")
    info(f"Klaster pokrenut (port {lab.port}, socket {lab.socket}).")
    dt, _ = pgbr(lab, "stanza-create", show_filter=r"stanza-create command end|ERROR|WARN")
    info(f"stanza-create: {dt:.2f} s")
    dt, _ = pgbr(lab, "check", show_filter=r"check command end|WAL segment|ERROR|WARN")
    info(f"check (provera arhiviranja WAL-a): {dt:.2f} s")

    conn = connect(lab)

    # ------------------------------------------------------------------ 4
    korak(4, f"Punjenje test podataka (~{args.scale} MB)")
    rows_per_mb = 6000  # ~170 B po redu uključujući zaglavlje torke
    n_big = int(args.scale * 0.6 * rows_per_mb)
    n_hot = int(args.scale * 0.3 * rows_per_mb)
    n_log = int(args.scale * 0.1 * rows_per_mb)
    t0 = time.perf_counter()
    sql(conn, """
        CREATE TABLE big_static (id bigint PRIMARY KEY, created date, payload text);
        CREATE TABLE hot_table  (id bigint PRIMARY KEY, balance numeric(12,2), updated timestamptz, payload text);
        CREATE TABLE event_log  (id bigint PRIMARY KEY, happened timestamptz, event text);
    """)
    sql(conn, f"""INSERT INTO big_static
                  SELECT i, DATE '2020-01-01' + (i % 1500)::int, repeat(md5(i::text), 3)
                  FROM generate_series(1, {n_big}) i""")
    sql(conn, f"""INSERT INTO hot_table
                  SELECT i, round((random() * 10000)::numeric, 2), now(), repeat(md5((i * 7)::text), 3)
                  FROM generate_series(1, {n_hot}) i""")
    sql(conn, f"""INSERT INTO event_log
                  SELECT i, now() - (i || ' seconds')::interval, 'event ' || repeat(md5(i::text), 3)
                  FROM generate_series(1, {n_log}) i""")
    # VACUUM FREEZE: postavlja hint bitove i visibility map ODMAH, da kasnija
    # čitanja ne bi menjala stranice (i time "lažno" menjala fajlove za inkrement).
    sql(conn, "VACUUM (FREEZE, ANALYZE)")
    sql(conn, "CHECKPOINT")
    load_dt = time.perf_counter() - t0
    size_rows = []
    for t in TABLES:
        path, size = relation_files(conn, t)
        size_rows.append((t, f"{sql(conn, f'SELECT count(*) FROM {t}', fetch=True)[0][0]:,}", mb(size), path))
    print_table(["tabela", "redova", "veličina", "fajl (relativno u PGDATA)"], size_rows)
    total_db = db_size(conn)
    info(f"Veličina baze: {mb(total_db)}; učitavanje: {load_dt:.1f} s")
    info("Uloge tabela: big_static se NIKAD ne menja, hot_table dobija male izmene, event_log samo INSERT.")

    backups = []          # rezultati svih backup-a (za tabelu/grafike)
    fingerprints = {}     # kontrolne sume podataka u trenutku backup-a

    def do_backup(kind, label, note):
        fp = table_fingerprint(conn)
        size_now = db_size(conn)
        for repo in lab.repos():
            info(f"pgbackrest --type={kind} --repo={repo} backup ...")
            dt, _ = pgbr(lab, f"--type={kind}", f"--repo={repo}", "backup",
                         show_filter=r"(full|diff|incr) backup size|new backup label|ERROR|WARN")
            b = latest_backup(lab, repo)
            binfo = b.get("info", {})
            repo_info = binfo.get("repository", {})
            rec = {
                "name": label, "type": kind, "repo": repo, "label": b["label"],
                "duration": dt, "db_size": binfo.get("size", size_now),
                "delta": binfo.get("delta", 0), "repo_delta": repo_info.get("delta", 0),
                "repo_size": repo_info.get("size", 0),
                "reference": b.get("reference") or [], "prior": b.get("prior"), "note": note,
                "start": b.get("timestamp", {}).get("start"), "stop": b.get("timestamp", {}).get("stop"),
            }
            backups.append(rec)
            fingerprints[(label, repo)] = fp
            ref = f", zavisi od: {rec['prior']}" if rec["prior"] else ""
            info(f"  -> {b['label']}: {dt:.2f} s, kopirano {mb(rec['delta'])} od {mb(rec['db_size'])}, "
                 f"u repozitorijumu {mb(rec['repo_delta'])}{ref}")
        return fp

    # ------------------------------------------------------------------ 5
    korak(5, "FULL backup")
    info("Full backup kopira SVE fajlove klastera - nezavisan je od drugih backup-a.")
    do_backup("full", "FULL", "kompletna kopija")

    # ------------------------------------------------------------------ 6
    korak(6, "INCR #1 - posle malog INSERT-a u event_log")
    n_new = max(1000, n_log // 20)
    sql(conn, f"""INSERT INTO event_log
                  SELECT {n_log} + i, now(), 'novi dogadjaj ' || md5(i::text)
                  FROM generate_series(1, {n_new}) i""")
    sql(conn, "CHECKPOINT")
    info(f"Dodato {n_new:,} redova u event_log (~{100 * n_new / (n_big + n_hot + n_log):.1f}% redova baze).")
    info("Incremental backup kopira samo fajlove promenjene od PRETHODNOG backup-a (bilo kog tipa).")
    do_backup("incr", "INCR #1", "INSERT u event_log")

    # ------------------------------------------------------------------ 7
    korak(7, "INCR #2 - posle UPDATE-a 1% redova u hot_table")
    n_upd = max(100, n_hot // 100)
    sql(conn, f"UPDATE hot_table SET balance = balance + 1, updated = now() WHERE id <= {n_upd}")
    sql(conn, "CHECKPOINT")
    hot_path, hot_size = relation_files(conn, "hot_table")
    info(f"Izmenjeno {n_upd:,} redova (1%) u hot_table; fajl tabele {hot_path} ima {mb(hot_size)}.")
    info("pgBackRest (file-level) poredi fajlove po veličini/vremenu izmene: izmenjen fajl se kopira CEO,")
    info("iako je promenjen samo mali broj njegovih stranica (8 kB blokova).")
    do_backup("incr", "INCR #2", "UPDATE 1% hot_table")

    # ------------------------------------------------------------------ 8
    korak(8, "Restore INCR #2 u novi direktorijum i verifikacija")
    incr2 = next(b for b in backups if b["name"] == "INCR #2" and b["repo"] == 1)
    expected = fingerprints[("INCR #2", 1)]
    info(f"Vraća se backup {incr2['label']} (lanac: {' -> '.join(incr2['reference'] + [incr2['label']])})")
    info("--type=immediate: oporavak se zaustavlja čim baza postane konzistentna (stanje u trenutku backup-a).")
    lab.restore_data.mkdir(mode=0o700)
    restore_dt, _ = pgbr(lab, "--repo=1", f"--set={incr2['label']}", f"--pg1-path={lab.restore_data}",
                         "--type=immediate", "--target-action=promote", "--archive-mode=off", "restore",
                         show_filter=r"restore size|restore command end|ERROR|WARN")
    start_dt = pg_start(lab, lab.restore_data, lab.log / "postgres_restore.log",
                        extra_opts=f"-p {lab.restore_port} -c unix_socket_directories='{lab.socket_restore}' "
                                   f"-c archive_mode=off")
    rconn = None
    for _ in range(60):  # čekamo da se završi recovery i promocija
        try:
            rconn = connect(lab, restore=True)
            if not sql(rconn, "SELECT pg_is_in_recovery()", fetch=True)[0][0]:
                break
        except psycopg2.OperationalError:
            pass
        time.sleep(1)
    if rconn is None:
        sys.exit(f"Vraćeni klaster se nije pokrenuo - pogledajte {lab.log / 'postgres_restore.log'}")
    got = table_fingerprint(rconn)
    rconn.close()
    pg_stop(lab, lab.restore_data)
    restore_ok = got == expected
    info(f"pgbackrest restore: {restore_dt:.2f} s, start + WAL recovery: {start_dt:.2f} s")
    info(f"Očekivano : {fingerprint_str(expected)}")
    info(f"Vraćeno   : {fingerprint_str(got)}")
    info("VERIFIKACIJA USPEŠNA - podaci identični stanju u trenutku INCR #2." if restore_ok
         else "VERIFIKACIJA NEUSPEŠNA!")
    shutil.rmtree(lab.restore_data)
    restore_results = [{"name": f"Restore INCR #2 ({incr2['label']})", "restore": restore_dt,
                        "recovery": start_dt, "ok": restore_ok}]

    # ------------------------------------------------------------------ 9
    korak(9, "DIFF backup i INCR #3")
    info("Differential backup kopira sve što je promenjeno od poslednjeg FULL backup-a.")
    do_backup("diff", "DIFF", "sve promene od FULL")
    sql(conn, f"""INSERT INTO event_log
                  SELECT {n_log + n_new} + i, now(), 'jos dogadjaja ' || md5(i::text)
                  FROM generate_series(1, 1000) i""")
    sql(conn, "CHECKPOINT")
    do_backup("incr", "INCR #3", "INSERT posle DIFF-a")
    info("INCR #3 se oslanja na DIFF (a DIFF samo na FULL): za restore trebaju 3 backup-a, ne 5.")

    # ------------------------------------------------------------------ 10
    korak(10, "Point-in-time recovery (PITR) posle slučajnog DROP TABLE")
    before = table_fingerprint(conn)
    sql(conn, "SELECT pg_create_restore_point('pre_katastrofa')")
    info("Kreiran restore point 'pre_katastrofa'.")
    sql(conn, "DROP TABLE hot_table")
    info("!!! Simulirana ljudska greška: DROP TABLE hot_table")
    pgbr(lab, "check", quiet=True)  # prebacuje WAL segment i čeka da bude arhiviran
    conn.close()
    pg_stop(lab, lab.data)
    info("Klaster zaustavljen; restore u ISTI direktorijum sa --delta (menjaju se samo razlike).")
    pitr_dt, _ = pgbr(lab, "--repo=1", "--delta", "--type=name", "--target=pre_katastrofa",
                      "--target-action=promote", "restore",
                      show_filter=r"restore size|restore command end|ERROR|WARN")
    start_dt = pg_start(lab, lab.data, lab.log / "postgres.log")
    conn = None
    for _ in range(60):
        try:
            conn = connect(lab)
            if not sql(conn, "SELECT pg_is_in_recovery()", fetch=True)[0][0]:
                break
        except psycopg2.OperationalError:
            pass
        time.sleep(1)
    if conn is None:
        sys.exit(f"Klaster se nije pokrenuo posle PITR-a - pogledajte {lab.log / 'postgres.log'}")
    after = table_fingerprint(conn)
    pitr_ok = after == before
    timeline = sql(conn, "SELECT timeline_id FROM pg_control_checkpoint()", fetch=True)[0][0]
    info(f"restore: {pitr_dt:.2f} s, start + replay WAL-a do restore point-a: {start_dt:.2f} s")
    info(f"Pre greške : {fingerprint_str(before)}")
    info(f"Posle PITR : {fingerprint_str(after)}")
    info(f"Nova vremenska linija (timeline): {timeline}")
    info("PITR USPEŠAN - hot_table je vraćena." if pitr_ok else "PITR NEUSPEŠAN!")
    restore_results.append({"name": "PITR do 'pre_katastrofa' (--delta)", "restore": pitr_dt,
                            "recovery": start_dt, "ok": pitr_ok})

    # ------------------------------------------------------------------ 11
    korak(11, "Politika zadržavanja (retention) i zavisnosti backup-a")
    _, info_before = pgbr(lab, "--repo=1", "info", quiet=True)
    info("Pravi se novi FULL backup, zatim expire sa repo1-retention-full=1:")
    dt, _ = pgbr(lab, "--repo=1", "--type=full", "backup", show_filter=r"new backup label|ERROR|WARN")
    labels_before = [b["label"] for b in backup_info(lab, 1)]
    pgbr(lab, "--repo=1", "--repo1-retention-full=1", "expire",
         show_filter=r"expire (full|diff|incr)|remove archive|ERROR|WARN")
    labels_after = [b["label"] for b in backup_info(lab, 1)]
    expired = [lb for lb in labels_before if lb not in labels_after]
    info(f"Obrisano {len(expired)} backup-a: {', '.join(expired)}")
    info("Zajedno sa starim FULL-om obrisani su i svi DIFF/INCR backup-i koji od njega zavise -")
    info("inkrementalni backup bez svog punog backup-a nije upotrebljiv.")
    _, info_text = pgbr(lab, "info", quiet=True)

    # ------------------------------------------------------------------ 12
    korak(12, "Zbirni rezultati")
    headers = ["backup", "repo", "tip", "trajanje", "veličina baze", "kopirano (delta)", "% baze",
               "u repozitorijumu", "zavisi od"]
    rows = []
    for b in backups:
        pct = 100 * b["delta"] / b["db_size"] if b["db_size"] else 0
        repo_kind = "file" if b["repo"] == 1 else "block"
        rows.append([b["name"], f"{b['repo']} ({repo_kind})", b["type"].upper(), f"{b['duration']:.2f} s",
                     mb(b["db_size"]), mb(b["delta"]), f"{pct:.1f}%", mb(b["repo_delta"]),
                     b["prior"] or "-"])
    print_table(headers, rows)
    print()
    print_table(["restore test", "pgbackrest restore", "start + recovery", "verifikacija"],
                [(r["name"], f"{r['restore']:.2f} s", f"{r['recovery']:.2f} s", "OK" if r["ok"] else "GREŠKA")
                 for r in restore_results])

    # CSV
    csv_path = out_dir / "backupi.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["backup", "repo", "tip", "label", "trajanje_s", "velicina_baze_B", "delta_B",
                    "repo_delta_B", "repo_size_B", "prior", "reference"])
        for b in backups:
            w.writerow([b["name"], b["repo"], b["type"], b["label"], f"{b['duration']:.3f}", b["db_size"],
                        b["delta"], b["repo_delta"], b["repo_size"], b["prior"] or "", " ".join(b["reference"])])
    (out_dir / "pgbackrest_info.txt").write_text(
        "=== pre expire (repo1) ===\n" + info_before + "\n=== kraj eksperimenta ===\n" + info_text,
        encoding="utf-8")

    charts = make_charts(out_dir, backups, lab.block_incr)

    md = []
    md.append("# Full vs Incremental backup (pgBackRest) - rezultati\n")
    md.append(f"Datum: {started:%d.%m.%Y %H:%M} · {pgbr_version} · {pg_version} · kompresija: {lab.compress}"
              f" · veličina baze: {mb(total_db)}\n")
    md.append("## 1. Test podaci\n")
    md.append(md_table(["tabela", "redova", "veličina", "fajl"], size_rows))
    md.append("\n## 2. Scenario\n")
    md.append("1. **FULL** - kompletna kopija klastera.\n"
              "2. **INCR #1** - posle INSERT-a u `event_log`.\n"
              "3. **INCR #2** - posle UPDATE-a 1% redova u `hot_table`.\n"
              "4. Restore INCR #2 u novi direktorijum + verifikacija kontrolnih suma.\n"
              "5. **DIFF** - sve promene od FULL-a; **INCR #3** - mala promena posle DIFF-a.\n"
              "6. PITR: restore point -> `DROP TABLE hot_table` -> restore do restore point-a.\n"
              "7. Retention: novi FULL + `expire --repo1-retention-full=1`.\n")
    md.append("## 3. Backup-i\n")
    md.append(md_table(headers, rows))
    md.append("\n- *kopirano (delta)* - nekompresovana količina podataka koju je backup zaista kopirao;\n"
              "- *u repozitorijumu* - zauzeće tog backup-a u repozitorijumu (posle kompresije);\n"
              "- *zavisi od* - prethodni backup u lancu (potreban za restore).\n")
    md.append("## 4. Restore testovi\n")
    md.append(md_table(["test", "pgbackrest restore", "start + WAL recovery", "verifikacija"],
                       [(r["name"], f"{r['restore']:.2f} s", f"{r['recovery']:.2f} s",
                         "OK" if r["ok"] else "GREŠKA") for r in restore_results]))
    md.append("\n## 5. Retention\n")
    md.append(f"Posle novog FULL backup-a i `expire` sa `repo1-retention-full=1` obrisano je "
              f"{len(expired)} backup-a: {', '.join(f'`{e}`' for e in expired)}.\n")
    md.append("## 6. Zapažanja\n")
    full1 = next(b for b in backups if b["name"] == "FULL" and b["repo"] == 1)
    i2 = next(b for b in backups if b["name"] == "INCR #2" and b["repo"] == 1)
    md.append(f"- INCR #2 je nastao posle izmene 1% redova `hot_table`, a kopirao je {mb(i2['delta'])} "
              f"({100 * i2['delta'] / full1['delta']:.0f}% full backup-a) - pgBackRest inkrement je na nivou "
              f"**fajla**, a `hot_table` je jedan fajl od {mb(hot_size)}.")
    if lab.block_incr:
        i2b = next(b for b in backups if b["name"] == "INCR #2" and b["repo"] == 2)
        md.append(f"- Sa block incremental-om (repo2) isti INCR #2 u repozitorijumu zauzima {mb(i2b['repo_delta'])} "
                  f"umesto {mb(i2['repo_delta'])} - čuvaju se samo izmenjeni blokovi.")
    md.append("- DIFF kopira sve promene od FULL-a (raste vremenom), ali restore zahteva samo FULL + DIFF.")
    md.append("- INCR je najmanji i najbrži, ali restore zahteva ceo lanac FULL -> ... -> INCR.")
    md.append("\n## 7. Fajlovi\n")
    md.append("- `backupi.csv` - sirovi rezultati\n- `pgbackrest_info.txt` - izlaz `pgbackrest info`")
    for c in charts:
        md.append(f"- `{c.name}`\n\n![{c.stem}]({c.name})")
    report = out_dir / "izvestaj.md"
    report.write_text("\n".join(md) + "\n", encoding="utf-8")

    print()
    info(f"Rezultati sačuvani u: {out_dir}")
    for f in [report, csv_path, out_dir / "pgbackrest_info.txt", *charts]:
        info(f"  - {f.name}")

    conn.close()
    if args.cleanup:
        pg_stop(lab, lab.data, check=False)
        shutil.rmtree(lab.root)
        info(f"Laboratorija obrisana ({lab.root}).")
    elif args.keep_running:
        info(f"Klaster ostaje pokrenut: psql -h {lab.socket} -p {lab.port} -d postgres")
        info(f"pgBackRest: pgbackrest --config={lab.conf} --stanza={STANZA} info")
    else:
        pg_stop(lab, lab.data, check=False)
        info(f"Klaster zaustavljen; fajlovi laboratorije ostaju u {lab.root} (--cleanup za brisanje).")
    info(f"Ukupno trajanje: {(datetime.now() - started).total_seconds():.0f} s")


# ---------------------------------------------------------------------------
# Grafici
# ---------------------------------------------------------------------------

PALETTE = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]
INK, INK2, MUTED, GRID, AXIS, SURFACE = "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7", "#fcfcfb"


def style_axes(ax, title, ylabel):
    ax.set_facecolor(SURFACE)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(AXIS)
    ax.tick_params(colors=MUTED, labelcolor=INK2)
    ax.yaxis.grid(True, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.set_title(title, color=INK, fontsize=12, loc="left", pad=12)
    ax.set_ylabel(ylabel, color=INK2)


def make_charts(out_dir, backups, block_incr):
    if not HAVE_MPL:
        info("matplotlib nije instaliran - grafici se preskaču (pip install matplotlib).")
        return []
    files = []
    names = []
    for b in backups:
        if b["name"] not in names:
            names.append(b["name"])

    def series(repo, key):
        vals = []
        for n in names:
            b = next((x for x in backups if x["name"] == n and x["repo"] == repo), None)
            vals.append(b[key] if b else 0)
        return vals

    # 1) Veličina: kopirano vs u repozitorijumu
    fig, ax = plt.subplots(figsize=(10, 5), facecolor=SURFACE)
    groups = [("kopirano (nekompresovano)", series(1, "delta"))]
    groups.append(("repo1 file-level (kompresovano)", series(1, "repo_delta")))
    if block_incr:
        groups.append(("repo2 block incremental (kompresovano)", series(2, "repo_delta")))
    n = len(groups)
    width = min(0.8 / n, 0.26)
    for gi, (label, vals) in enumerate(groups):
        xs = [i + (gi - (n - 1) / 2) * width for i in range(len(names))]
        mbv = [v / 1024 / 1024 for v in vals]
        bars = ax.bar(xs, mbv, width * 0.92, color=PALETTE[gi], label=label)
        for bar, v in zip(bars, mbv):
            ax.text(bar.get_x() + bar.get_width() / 2, v, f"{v:.1f}", ha="center", va="bottom",
                    color=INK2, fontsize=7.5)
    db = series(1, "db_size")
    ax.axhline(max(db) / 1024 / 1024, color=MUTED, linewidth=1)
    ax.text(len(names) - 0.5, max(db) / 1024 / 1024 * 1.02, "veličina baze", color=INK2, fontsize=8, ha="right",
            va="bottom")
    ax.set_xticks(range(len(names)))
    ax.set_xticklabels(names)
    ax.set_ylim(0, max(db) / 1024 / 1024 * 1.15)
    style_axes(ax, "Veličina backup-a po tipu", "MB")
    ax.legend(frameon=False, labelcolor=INK2, fontsize=9, ncol=len(groups), loc="upper center",
              bbox_to_anchor=(0.5, -0.08))
    fig.tight_layout()
    f = out_dir / "grafik_velicina_backupa.png"
    fig.savefig(f, dpi=150, facecolor=SURFACE)
    plt.close(fig)
    files.append(f)

    # 2) Trajanje
    fig, ax = plt.subplots(figsize=(10, 4.6), facecolor=SURFACE)
    tgroups = [("repo1 file-level", series(1, "duration"))]
    if block_incr:
        tgroups.append(("repo2 block incremental", series(2, "duration")))
    n = len(tgroups)
    width = min(0.8 / n, 0.3)
    for gi, (label, vals) in enumerate(tgroups):
        xs = [i + (gi - (n - 1) / 2) * width for i in range(len(names))]
        bars = ax.bar(xs, vals, width * 0.92, color=PALETTE[gi + 1], label=label)
        for bar, v in zip(bars, vals):
            ax.text(bar.get_x() + bar.get_width() / 2, v, f"{v:.1f} s", ha="center", va="bottom",
                    color=INK2, fontsize=8)
    ax.set_xticks(range(len(names)))
    ax.set_xticklabels(names)
    ax.set_ylim(0, max(max(v) for _, v in tgroups) * 1.2)
    style_axes(ax, "Trajanje backup-a po tipu", "sekunde")
    if n > 1:
        ax.legend(frameon=False, labelcolor=INK2, fontsize=9)
    fig.tight_layout()
    f = out_dir / "grafik_trajanje_backupa.png"
    fig.savefig(f, dpi=150, facecolor=SURFACE)
    plt.close(fig)
    files.append(f)
    return files


if __name__ == "__main__":
    main()
