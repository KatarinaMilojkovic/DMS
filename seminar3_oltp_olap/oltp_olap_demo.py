#!/usr/bin/env python3
"""
Seminarski rad: OLTP i OLAP sistemi
===================================

Skripta na PostgreSQL-u demonstrira razlike između transakcionih (OLTP) i
analitičkih (OLAP) sistema - od modela podataka, preko radnog opterećenja,
do toga zašto se u praksi razdvajaju:

  KORAK 1  Priprema baze
  KORAK 2  OLTP šema u 3NF (prodavnica) + istorijski podaci
  KORAK 3  OLTP radno opterećenje: mnogo kratkih, konkurentnih transakcija
           (TPS, latencija, rollback) + provera ACID svojstava
  KORAK 4  Obrasci pristupa podacima: OLTP upit vs analitički upit (EXPLAIN)
  KORAK 5  ETL: izgradnja skladišta podataka (zvezdasta šema / star schema)
  KORAK 6  OLAP upiti: ROLLUP, CUBE, slice, dice, drill-down, top-N, YoY;
           isto pitanje nad 3NF šemom vs star šemom vs materijalizovanim pogledom
  KORAK 7  Interferencija: OLTP sam vs OLTP + OLAP upiti na istom sistemu
  KORAK 8  Inkrementalni ETL i kašnjenje podataka u skladištu
  KORAK 9  Zbirni rezultati, grafici i izveštaj

Pokretanje (primer):
    python oltp_olap_demo.py --host localhost --user postgres --password postgres --scale 1 --duration 15
"""

import argparse
import csv
import json
import os
import random
import statistics
import sys
import threading
import time
from datetime import datetime
from decimal import Decimal
from pathlib import Path

try:
    import psycopg2
    import psycopg2.errors
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


# ---------------------------------------------------------------------------
# Pomoćne funkcije
# ---------------------------------------------------------------------------

def korak(broj, naslov):
    print()
    print("=" * 78)
    print(f"=== KORAK {broj}: {naslov}")
    print("=" * 78)


def info(tekst=""):
    print(f"    {tekst}")


def fmt_num(v, dec=0):
    if v is None:
        return "-"
    return f"{float(v):,.{dec}f}"


def fmt_cell(v):
    """Formatira vrednost iz rezultata upita (NULL iz ROLLUP/CUBE -> '*')."""
    if v is None:
        return "*"
    if isinstance(v, Decimal):
        return fmt_num(v, 1 if v.as_tuple().exponent == -1 else 0)
    if isinstance(v, float):
        return fmt_num(v)
    return str(v)


def fmt_bytes(n):
    for unit in ("B", "kB", "MB", "GB"):
        if abs(n) < 1024 or unit == "GB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024
    return f"{n:.1f} GB"


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


def connect(args, dbname=None, autocommit=True):
    conn = psycopg2.connect(host=args.host, port=args.port, user=args.user,
                            password=args.password, dbname=dbname or args.db)
    conn.autocommit = autocommit
    return conn


def execute(conn, sql, params=None):
    t0 = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(sql, params)
    return time.perf_counter() - t0


def query(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def timed_query(conn, sql, runs):
    """1 zagrevanje + runs merenja; vraća (medijana_ms, rezultat)."""
    times, rows = [], None
    for i in range(runs + 1):
        t0 = time.perf_counter()
        rows = query(conn, sql)
        if i:
            times.append((time.perf_counter() - t0) * 1000)
    return statistics.median(times), rows


def percentile(values, p):
    if not values:
        return 0.0
    s = sorted(values)
    k = (len(s) - 1) * p / 100
    f, c = int(k), min(int(k) + 1, len(s) - 1)
    return s[f] + (s[c] - s[f]) * (k - f)


# ---------------------------------------------------------------------------
# KORAK 2: OLTP šema (3NF)
# ---------------------------------------------------------------------------
# Normalizovana šema: svaki podatak se čuva na jednom mestu (region -> grad ->
# kupac; kategorija -> proizvod), a integritet obezbeđuju PK/FK/CHECK
# ograničenja. Optimizovana je za brz upis i ažuriranje malog broja redova.

OLTP_DDL = """
CREATE SCHEMA oltp;

CREATE TABLE oltp.region (
    region_id   serial PRIMARY KEY,
    name        text NOT NULL UNIQUE
);

CREATE TABLE oltp.city (
    city_id     serial PRIMARY KEY,
    name        text NOT NULL,
    region_id   int  NOT NULL REFERENCES oltp.region
);

CREATE TABLE oltp.customer (
    customer_id serial PRIMARY KEY,
    first_name  text NOT NULL,
    last_name   text NOT NULL,
    email       text NOT NULL UNIQUE,
    city_id     int  NOT NULL REFERENCES oltp.city,
    created_at  timestamptz NOT NULL
);

CREATE TABLE oltp.category (
    category_id serial PRIMARY KEY,
    name        text NOT NULL UNIQUE
);

CREATE TABLE oltp.product (
    product_id  serial PRIMARY KEY,
    name        text NOT NULL,
    category_id int  NOT NULL REFERENCES oltp.category,
    price       numeric(10,2) NOT NULL CHECK (price > 0),
    stock       int  NOT NULL CHECK (stock >= 0)          -- zaliha nikad ne sme biti negativna
);

CREATE TABLE oltp.orders (
    order_id    bigserial PRIMARY KEY,
    customer_id int  NOT NULL REFERENCES oltp.customer,
    order_date  timestamptz NOT NULL,
    status      text NOT NULL CHECK (status IN ('PLACENA','POSLATA','ISPORUCENA','OTKAZANA')),
    total       numeric(12,2) NOT NULL DEFAULT 0
);

CREATE TABLE oltp.order_item (
    order_id    bigint   NOT NULL REFERENCES oltp.orders,
    line_no     smallint NOT NULL,
    product_id  int      NOT NULL REFERENCES oltp.product,
    quantity    int      NOT NULL CHECK (quantity > 0),
    unit_price  numeric(10,2) NOT NULL,
    PRIMARY KEY (order_id, line_no)
);

CREATE TABLE oltp.payment (
    payment_id  bigserial PRIMARY KEY,
    order_id    bigint NOT NULL UNIQUE REFERENCES oltp.orders,
    amount      numeric(12,2) NOT NULL,
    method      text NOT NULL,
    paid_at     timestamptz NOT NULL
);

-- indeksi nad stranim ključevima (za brze pretrage po kupcu / proizvodu)
CREATE INDEX idx_orders_customer   ON oltp.orders (customer_id);
CREATE INDEX idx_order_item_product ON oltp.order_item (product_id);
"""

REGIONS = {
    "Beogradski region": ["Beograd"],
    "Region Vojvodine": ["Novi Sad", "Subotica", "Zrenjanin", "Pančevo", "Sombor"],
    "Region Šumadije i Zapadne Srbije": ["Kragujevac", "Čačak", "Kraljevo", "Užice", "Šabac", "Valjevo"],
    "Region Južne i Istočne Srbije": ["Niš", "Leskovac", "Vranje", "Zaječar", "Pirot"],
}
CATEGORIES = ["Elektronika", "Knjige", "Odeća", "Sport", "Kućni aparati", "Igračke", "Hrana", "Kozmetika"]
FIRST_NAMES = ["Ana", "Marko", "Jelena", "Nikola", "Milica", "Stefan", "Katarina", "Luka", "Jovana", "Petar",
               "Marija", "Nemanja", "Teodora", "Aleksa", "Sara", "Vuk", "Dragana", "Miloš", "Ivana", "Filip"]
LAST_NAMES = ["Jovanović", "Petrović", "Nikolić", "Marković", "Đorđević", "Stojanović", "Ilić", "Stanković",
              "Pavlović", "Milošević", "Popović", "Savić", "Kostić", "Milojković", "Todorović", "Lazić"]


def pg_array(values):
    return "(ARRAY[" + ",".join("'" + v.replace("'", "''") + "'" for v in values) + "])"


def load_oltp_data(conn, scale):
    n_customers = max(1000, int(20000 * scale))
    n_products = max(200, int(2000 * scale))
    n_orders = max(10000, int(200000 * scale))
    steps = []
    execute(conn, "SELECT setseed(0.2026)")

    region_values = ",".join(f"('{r}')" for r in REGIONS)
    city_values = ",".join(f"('{c}', {ri + 1})" for ri, cities in enumerate(REGIONS.values()) for c in cities)
    n_cities = sum(len(c) for c in REGIONS.values())
    cat_values = ",".join(f"('{c}')" for c in CATEGORIES)

    steps.append(("region, grad, kategorija", f"""
        INSERT INTO oltp.region (name) VALUES {region_values};
        INSERT INTO oltp.city (name, region_id) VALUES {city_values};
        INSERT INTO oltp.category (name) VALUES {cat_values};"""))
    # Beograd (city_id = 1) dobija najviše kupaca: random()^1.6 favorizuje male id-jeve
    steps.append(("customer", f"""
        INSERT INTO oltp.customer (first_name, last_name, email, city_id, created_at)
        SELECT {pg_array(FIRST_NAMES)}[1 + floor(random() * {len(FIRST_NAMES)})::int],
               {pg_array(LAST_NAMES)}[1 + floor(random() * {len(LAST_NAMES)})::int],
               'kupac' || i || '@primer.rs',
               1 + floor({n_cities} * random() ^ 1.6)::int,
               now() - random() * interval '4 years'
        FROM generate_series(1, {n_customers}) AS i"""))
    steps.append(("product", f"""
        INSERT INTO oltp.product (name, category_id, price, stock)
        SELECT 'Proizvod ' || lpad(i::text, 5, '0'),
               1 + (i % {len(CATEGORIES)}),
               round((200 + random() ^ 2 * 49800)::numeric, 2),
               100 + floor(random() * 900)::int
        FROM generate_series(1, {n_products}) AS i"""))
    # Istorija od 1. januara pre 3 godine do juče. Datum raste sa order_id, a
    # power(u, 0.75) daje rast broja narudžbina kroz vreme (trend ~ +20-50% godišnje).
    steps.append(("orders", f"""
        INSERT INTO oltp.orders (order_id, customer_id, order_date, status)
        SELECT i, 1 + floor(random() * {n_customers})::int, d,
               CASE WHEN random() < 0.03 THEN 'OTKAZANA'
                    WHEN d > now() - interval '3 days' THEN 'PLACENA'
                    WHEN d > now() - interval '8 days' THEN 'POSLATA'
                    ELSE 'ISPORUCENA' END
        FROM (SELECT i, h.start_ts + (h.end_ts - h.start_ts) * power(i::float8 / {n_orders}, 0.75) AS d
              FROM generate_series(1, {n_orders}) AS i,
                   (SELECT date_trunc('year', now() - interval '3 years') AS start_ts,
                           now() - interval '1 day' AS end_ts) AS h
              OFFSET 0) AS g;
        SELECT setval(pg_get_serial_sequence('oltp.orders', 'order_id'), {n_orders})"""))
    # Popularnost proizvoda je neravnomerna (random()^2); u novembru/decembru se kupuje više
    steps.append(("order_item", f"""
        INSERT INTO oltp.order_item (order_id, line_no, product_id, quantity, unit_price)
        SELECT g.order_id, g.line_no, g.product_id, g.quantity, p.price
        FROM (SELECT o.order_id, n AS line_no,
                     1 + floor({n_products} * random() ^ 2)::int AS product_id,
                     1 + floor(random() * 3)::int
                       + CASE WHEN extract(month FROM o.order_date) IN (11, 12) THEN 1 ELSE 0 END AS quantity
              FROM oltp.orders AS o,
                   generate_series(1, 1 + (o.order_id * 7919 % 4)::int) AS n
              OFFSET 0) AS g
        JOIN oltp.product AS p ON p.product_id = g.product_id"""))
    steps.append(("orders.total", """
        UPDATE oltp.orders AS o SET total = s.total
        FROM (SELECT order_id, SUM(quantity * unit_price) AS total
              FROM oltp.order_item GROUP BY order_id) AS s
        WHERE s.order_id = o.order_id"""))
    steps.append(("payment", """
        INSERT INTO oltp.payment (order_id, amount, method, paid_at)
        SELECT order_id, total,
               (ARRAY['KARTICA','POUZECE','PAYPAL','UPLATNICA'])[1 + floor(random() * 4)::int],
               order_date + random() * interval '1 hour'
        FROM oltp.orders WHERE status <> 'OTKAZANA'"""))
    steps.append(("VACUUM ANALYZE", "VACUUM ANALYZE"))

    for name, sql in steps:
        dt = execute(conn, sql)
        info(f"{name:<26} {dt:6.2f} s")
    return n_customers, n_products, n_orders


# ---------------------------------------------------------------------------
# KORAK 3: OLTP radno opterećenje
# ---------------------------------------------------------------------------
# Tri tipa kratkih transakcija (po uzoru na TPC-C):
#   nova_narudzbina (50%) - zaključava zalihe (SELECT ... FOR UPDATE), upisuje
#                           narudžbinu, stavke i uplatu, umanjuje zalihe
#   status_narudzbine (30%) - čitanje jedne narudžbine po primarnom ključu
#   isporuka (20%)        - ažuriranje statusa jedne narudžbine

TX_LABELS = {"nova_narudzbina": "nova narudžbina", "status_narudzbine": "status narudžbine",
             "isporuka": "isporuka"}


class WorkerStats:
    def __init__(self):
        self.lat = {"nova_narudzbina": [], "status_narudzbine": [], "isporuka": []}
        self.commits = 0
        self.rollbacks = 0
        self.errors = 0
        self.elapsed = 0.0


def tx_new_order(cur, conn, rnd, ctx):
    customer_id = rnd.randint(1, ctx["n_customers"])
    n_items = rnd.randint(1, 4)
    # Neravnomerna popularnost: mali id-jevi su "hit" proizvodi -> konkurencija za iste redove
    products = {1 + int(ctx["n_products"] * rnd.random() ** 2) for _ in range(n_items)}
    # Proizvodi se zaključavaju UVEK u rastućem redosledu id-ja -> nema deadlock-a
    items = [(pid, rnd.randint(1, 3)) for pid in sorted(products)]
    prices = {}
    for pid, qty in items:
        cur.execute("SELECT price, stock FROM oltp.product WHERE product_id = %s FOR UPDATE", (pid,))
        price, stock = cur.fetchone()
        if stock < qty:
            conn.rollback()  # atomičnost: ništa od započete narudžbine ne ostaje u bazi
            return False
        prices[pid] = price
    total = sum(prices[pid] * qty for pid, qty in items)
    cur.execute("""INSERT INTO oltp.orders (customer_id, order_date, status, total)
                   VALUES (%s, now(), 'PLACENA', %s) RETURNING order_id""", (customer_id, total))
    order_id = cur.fetchone()[0]
    args = []
    for line_no, (pid, qty) in enumerate(items, start=1):
        args += [order_id, line_no, pid, qty, prices[pid]]
    placeholders = ",".join(["(%s,%s,%s,%s,%s)"] * len(items))
    cur.execute(f"INSERT INTO oltp.order_item (order_id, line_no, product_id, quantity, unit_price) "
                f"VALUES {placeholders}", args)
    for pid, qty in items:
        cur.execute("UPDATE oltp.product SET stock = stock - %s WHERE product_id = %s", (qty, pid))
    cur.execute("""INSERT INTO oltp.payment (order_id, amount, method, paid_at)
                   VALUES (%s, %s, 'KARTICA', now())""", (order_id, total))
    conn.commit()
    return True


def tx_order_status(cur, conn, rnd, ctx):
    order_id = rnd.randint(1, ctx["max_order_id"])
    cur.execute("""SELECT o.order_id, o.status, o.total, c.first_name, c.last_name, count(i.line_no)
                   FROM oltp.orders o
                   JOIN oltp.customer c ON c.customer_id = o.customer_id
                   JOIN oltp.order_item i ON i.order_id = o.order_id
                   WHERE o.order_id = %s
                   GROUP BY o.order_id, c.customer_id""", (order_id,))
    cur.fetchall()
    conn.commit()
    return True


def tx_ship(cur, conn, rnd, ctx):
    order_id = rnd.randint(max(1, ctx["max_order_id"] - 5000), ctx["max_order_id"])
    cur.execute("UPDATE oltp.orders SET status = 'POSLATA' WHERE order_id = %s AND status = 'PLACENA'",
                (order_id,))
    conn.commit()
    return True


def oltp_worker(args, ctx, seed, duration, stats, start_barrier):
    conn = connect(args, autocommit=False)
    cur = conn.cursor()
    rnd = random.Random(seed)
    start_barrier.wait()  # sve niti počinju istovremeno, kada su konekcije uspostavljene
    stop_at = time.perf_counter() + duration
    while time.perf_counter() < stop_at:
        r = rnd.random()
        if r < 0.5:
            kind, fn = "nova_narudzbina", tx_new_order
        elif r < 0.8:
            kind, fn = "status_narudzbine", tx_order_status
        else:
            kind, fn = "isporuka", tx_ship
        t0 = time.perf_counter()
        try:
            ok = fn(cur, conn, rnd, ctx)
        except (psycopg2.errors.DeadlockDetected, psycopg2.errors.SerializationFailure):
            conn.rollback()
            stats.errors += 1
            continue
        stats.lat[kind].append((time.perf_counter() - t0) * 1000)
        if ok:
            stats.commits += 1
        else:
            stats.rollbacks += 1
    stats.elapsed = time.perf_counter() - (stop_at - duration)
    conn.close()


def olap_worker(args, sql, duration, counter, start_barrier):
    conn = connect(args)
    start_barrier.wait()
    stop_at = time.perf_counter() + duration
    while time.perf_counter() < stop_at:
        query(conn, sql)
        counter.append(1)
    conn.close()


def run_workload(args, ctx, duration, olap_sql=None, seed_base=100):
    """Pokreće args.threads OLTP niti (i opciono args.olap_threads OLAP niti)."""
    n_olap = args.olap_threads if olap_sql else 0
    barrier = threading.Barrier(args.threads + n_olap + 1)
    stats = [WorkerStats() for _ in range(args.threads)]
    olap_done = []
    jobs = [(oltp_worker, (args, ctx, seed_base + i, duration, stats[i], barrier)) for i in range(args.threads)]
    jobs += [(olap_worker, (args, olap_sql, duration, olap_done, barrier)) for _ in range(n_olap)]
    failures = []

    def guarded(fn, fn_args):
        try:
            fn(*fn_args)
        except Exception as e:  # noqa: BLE001 - greška se prijavljuje korisniku
            failures.append(e)
            barrier.abort()

    threads = [threading.Thread(target=guarded, args=job) for job in jobs]
    for t in threads:
        t.start()
    try:
        barrier.wait()
    except threading.BrokenBarrierError:
        pass
    for t in threads:
        t.join()
    if failures:
        raise RuntimeError(f"Greška u radnoj niti: {failures[0]}")
    # TPS se računa na osnovu stvarnog vremena rada OLTP niti
    elapsed = max(s.elapsed for s in stats)

    merged = WorkerStats()
    for s in stats:
        for k in merged.lat:
            merged.lat[k].extend(s.lat[k])
        merged.commits += s.commits
        merged.rollbacks += s.rollbacks
        merged.errors += s.errors
    all_lat = [v for k in merged.lat for v in merged.lat[k]]
    return {
        "elapsed": elapsed, "commits": merged.commits, "rollbacks": merged.rollbacks,
        "errors": merged.errors, "tps": merged.commits / elapsed,
        "p50": percentile(all_lat, 50), "p95": percentile(all_lat, 95), "p99": percentile(all_lat, 99),
        "by_type": {k: {"n": len(v), "p50": percentile(v, 50), "p95": percentile(v, 95),
                        "p99": percentile(v, 99), "avg": statistics.mean(v) if v else 0}
                    for k, v in merged.lat.items()},
        "olap_queries": len(olap_done),
    }


# ---------------------------------------------------------------------------
# KORAK 4: analiza planova izvršavanja
# ---------------------------------------------------------------------------

def plan_metrics(conn, sql):
    """EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) -> vreme, pročitani redovi, blokovi, tipovi čvorova."""
    rows = query(conn, f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}")
    plan = rows[0][0]
    if isinstance(plan, str):
        plan = json.loads(plan)
    root = plan[0]["Plan"]
    scanned, nodes = 0, []

    def walk(n):
        nonlocal scanned
        nt = n["Node Type"]
        if "Scan" in nt:
            loops = n.get("Actual Loops", 1)
            scanned += (n.get("Actual Rows", 0) + n.get("Rows Removed by Filter", 0)) * loops
            nodes.append(nt + (f" ({n['Relation Name']})" if "Relation Name" in n else ""))
        for c in n.get("Plans", []):
            walk(c)
    walk(root)
    return {
        "time": plan[0].get("Execution Time", root.get("Actual Total Time")),
        "rows_out": root.get("Actual Rows"),
        "scanned": int(scanned),
        "blocks": root.get("Shared Hit Blocks", 0) + root.get("Shared Read Blocks", 0),
        "nodes": nodes,
    }


# ---------------------------------------------------------------------------
# KORAK 5: skladište podataka (star schema) i ETL
# ---------------------------------------------------------------------------
# Denormalizovane dimenzije (grad i region su atributi kupca, kategorija je
# atribut proizvoda) i tabela činjenica sa merama (količina, prihod).
# Surogat ključevi (customer_key, product_key) odvajaju DW od ključeva izvora.

DW_DDL = """
CREATE SCHEMA dw;

CREATE TABLE dw.dim_date (
    date_key     int PRIMARY KEY,          -- YYYYMMDD
    full_date    date NOT NULL,
    year         int  NOT NULL,
    quarter      int  NOT NULL,
    month        int  NOT NULL,
    month_name   text NOT NULL,
    day          int  NOT NULL,
    day_of_week  int  NOT NULL,
    day_name     text NOT NULL,
    is_weekend   boolean NOT NULL
);

CREATE TABLE dw.dim_customer (
    customer_key serial PRIMARY KEY,       -- surogat ključ
    customer_id  int NOT NULL UNIQUE,      -- prirodni (poslovni) ključ iz OLTP-a
    full_name    text NOT NULL,
    city         text NOT NULL,
    region       text NOT NULL
);

CREATE TABLE dw.dim_product (
    product_key  serial PRIMARY KEY,
    product_id   int NOT NULL UNIQUE,
    product_name text NOT NULL,
    category     text NOT NULL,
    list_price   numeric(10,2) NOT NULL
);

CREATE TABLE dw.fact_sales (
    date_key     int NOT NULL REFERENCES dw.dim_date,
    customer_key int NOT NULL REFERENCES dw.dim_customer,
    product_key  int NOT NULL REFERENCES dw.dim_product,
    order_id     bigint NOT NULL,          -- degenerisana dimenzija
    line_no      smallint NOT NULL,
    quantity     int NOT NULL,
    unit_price   numeric(10,2) NOT NULL,
    revenue      numeric(12,2) NOT NULL
);

CREATE TABLE dw.etl_log (
    load_id      serial PRIMARY KEY,
    load_type    text NOT NULL,
    from_order   bigint NOT NULL,
    to_order     bigint NOT NULL,
    rows_loaded  bigint NOT NULL,
    loaded_at    timestamptz NOT NULL DEFAULT now()
);
"""

MONTHS = ["januar", "februar", "mart", "april", "maj", "jun", "jul", "avgust", "septembar", "oktobar",
          "novembar", "decembar"]
DAYS = ["ponedeljak", "utorak", "sreda", "četvrtak", "petak", "subota", "nedelja"]

ETL_DIMENSIONS = [
    ("dim_date", f"""
        INSERT INTO dw.dim_date
        SELECT to_char(d, 'YYYYMMDD')::int, d::date, extract(year FROM d)::int, extract(quarter FROM d)::int,
               extract(month FROM d)::int, {pg_array(MONTHS)}[extract(month FROM d)::int],
               extract(day FROM d)::int, extract(isodow FROM d)::int,
               {pg_array(DAYS)}[extract(isodow FROM d)::int], extract(isodow FROM d) IN (6, 7)
        FROM generate_series((SELECT min(order_date)::date FROM oltp.orders), current_date + 365,
                             interval '1 day') AS d"""),
    ("dim_customer", """
        INSERT INTO dw.dim_customer (customer_id, full_name, city, region)
        SELECT c.customer_id, c.first_name || ' ' || c.last_name, ci.name, r.name
        FROM oltp.customer c
        JOIN oltp.city ci ON ci.city_id = c.city_id
        JOIN oltp.region r ON r.region_id = ci.region_id
        ORDER BY c.customer_id"""),
    ("dim_product", """
        INSERT INTO dw.dim_product (product_id, product_name, category, list_price)
        SELECT p.product_id, p.name, cat.name, p.price
        FROM oltp.product p JOIN oltp.category cat ON cat.category_id = p.category_id
        ORDER BY p.product_id"""),
]

# Transformacija: spajanje zaglavlja i stavki, zamena prirodnih ključeva
# surogat ključevima, izračunavanje mere revenue, filtriranje otkazanih.
ETL_FACT = """
    INSERT INTO dw.fact_sales
    SELECT to_char(o.order_date, 'YYYYMMDD')::int, dc.customer_key, dp.product_key,
           o.order_id, i.line_no, i.quantity, i.unit_price, i.quantity * i.unit_price
    FROM oltp.orders o
    JOIN oltp.order_item i ON i.order_id = o.order_id
    JOIN dw.dim_customer dc ON dc.customer_id = o.customer_id
    JOIN dw.dim_product dp ON dp.product_id = i.product_id
    WHERE o.status <> 'OTKAZANA'
      AND o.order_id > %(od)s AND o.order_id <= %(do)s
    ORDER BY o.order_date, o.order_id, i.line_no
"""

MV_DDL = """
CREATE MATERIALIZED VIEW dw.mv_sales_month AS
SELECT d.year, d.quarter, d.month, c.region, p.category,
       SUM(f.revenue) AS revenue, SUM(f.quantity) AS quantity, COUNT(*) AS lines
FROM dw.fact_sales f
JOIN dw.dim_date d ON d.date_key = f.date_key
JOIN dw.dim_customer c ON c.customer_key = f.customer_key
JOIN dw.dim_product p ON p.product_key = f.product_key
GROUP BY d.year, d.quarter, d.month, c.region, p.category
"""


def etl_fact(conn, load_type):
    # Watermark = najveći order_id iz prethodnog učitavanja. Pojednostavljenje: u
    # produkciji se koristi CDC (npr. logička replikacija), jer transakcije mogu da
    # se potvrde van redosleda dodele order_id-ja, a menjaju se i postojeći redovi.
    from_id = query(conn, "SELECT COALESCE(max(to_order), 0) FROM dw.etl_log")[0][0]
    to_id = query(conn, "SELECT COALESCE(max(order_id), 0) FROM oltp.orders")[0][0]
    t0 = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(ETL_FACT, {"od": from_id, "do": to_id})
        n = cur.rowcount
        cur.execute("INSERT INTO dw.etl_log (load_type, from_order, to_order, rows_loaded) VALUES (%s,%s,%s,%s)",
                    (load_type, from_id, to_id, n))
    return time.perf_counter() - t0, n, from_id, to_id


# ---------------------------------------------------------------------------
# KORAK 6: OLAP upiti
# ---------------------------------------------------------------------------

STAR_JOIN = """
FROM dw.fact_sales f
JOIN dw.dim_date d     ON d.date_key = f.date_key
JOIN dw.dim_customer c ON c.customer_key = f.customer_key
JOIN dw.dim_product p  ON p.product_key = f.product_key
"""

OLAP_QUERIES = [
    ("ROLLUP (drill-down godina > kvartal > mesec)",
     "Hijerarhija vremena: međuzbirovi po godini i kvartalu + ukupan zbir.",
     f"""SELECT d.year, d.quarter, d.month, SUM(f.revenue) AS revenue
         {STAR_JOIN}
         GROUP BY ROLLUP (d.year, d.quarter, d.month)
         ORDER BY d.year NULLS LAST, d.quarter NULLS LAST, d.month NULLS LAST""",
     lambda rows: [r for r in rows if r[2] is None and r[1] is None]),
    ("CUBE (region × kategorija)",
     "Sve kombinacije agregacije: region, kategorija, region×kategorija i ukupno.",
     f"""SELECT c.region, p.category, SUM(f.revenue) AS revenue
         {STAR_JOIN}
         GROUP BY CUBE (c.region, p.category)
         ORDER BY c.region NULLS LAST, p.category NULLS LAST""",
     lambda rows: [r for r in rows if r[1] is None]),
    ("Slice (kategorija = Elektronika)",
     "Jedna vrednost jedne dimenzije -> 'isečak' kocke po regionu i godini.",
     f"""SELECT c.region, d.year, SUM(f.revenue) AS revenue
         {STAR_JOIN}
         WHERE p.category = 'Elektronika'
         GROUP BY c.region, d.year
         ORDER BY c.region, d.year""",
     lambda rows: rows[:6]),
    ("Dice (2 regiona × 2 kategorije × poslednja godina)",
     "Podskup vrednosti više dimenzija -> 'kockica' po kvartalu.",
     f"""SELECT c.region, p.category, d.quarter, SUM(f.revenue) AS revenue
         {STAR_JOIN}
         WHERE c.region IN ('Beogradski region', 'Region Vojvodine')
           AND p.category IN ('Elektronika', 'Sport')
           AND d.year = (SELECT max(year) - 1 FROM dw.dim_date d2
                         WHERE d2.date_key IN (SELECT date_key FROM dw.fact_sales))
         GROUP BY c.region, p.category, d.quarter
         ORDER BY 1, 2, 3""",
     lambda rows: rows[:6]),
    ("Top-3 proizvoda po kategoriji (RANK)",
     "Prozorska funkcija RANK() OVER (PARTITION BY kategorija).",
     f"""SELECT category, product_name, revenue, rnk FROM (
             SELECT p.category, p.product_name, SUM(f.revenue) AS revenue,
                    RANK() OVER (PARTITION BY p.category ORDER BY SUM(f.revenue) DESC) AS rnk
             {STAR_JOIN}
             GROUP BY p.category, p.product_name) t
         WHERE rnk <= 3
         ORDER BY category, rnk""",
     lambda rows: rows[:6]),
    ("Međugodišnji rast (LAG, YoY %)",
     "Poređenje sa prethodnom godinom pomoću LAG().",
     f"""SELECT category, year, revenue,
                ROUND(100.0 * (revenue - LAG(revenue) OVER w) / LAG(revenue) OVER w, 1) AS yoy_pct
         FROM (SELECT p.category, d.year, SUM(f.revenue) AS revenue
               {STAR_JOIN}
               GROUP BY p.category, d.year) t
         WINDOW w AS (PARTITION BY category ORDER BY year)
         ORDER BY category, year""",
     lambda rows: rows[:6]),
    ("Kumulativni prihod po mesecima (poslednjih 12)",
     "SUM() OVER (ORDER BY ...) - tekući zbir; filter po date_key koristi BRIN indeks.",
     f"""SELECT d.year, d.month, SUM(f.revenue) AS revenue,
                SUM(SUM(f.revenue)) OVER (ORDER BY d.year, d.month) AS cumulative
         {STAR_JOIN}
         WHERE f.date_key >= to_char(current_date - interval '12 months', 'YYYYMMDD')::int
         GROUP BY d.year, d.month
         ORDER BY d.year, d.month""",
     lambda rows: rows[-6:]),
]

# Isto poslovno pitanje: "prihod po regionu, kategoriji i godini"
Q_3NF = """
SELECT r.name AS region, cat.name AS category, extract(year FROM o.order_date)::int AS year,
       SUM(i.quantity * i.unit_price) AS revenue
FROM oltp.orders o
JOIN oltp.order_item i ON i.order_id = o.order_id
JOIN oltp.product p    ON p.product_id = i.product_id
JOIN oltp.category cat ON cat.category_id = p.category_id
JOIN oltp.customer c   ON c.customer_id = o.customer_id
JOIN oltp.city ci      ON ci.city_id = c.city_id
JOIN oltp.region r     ON r.region_id = ci.region_id
WHERE o.status <> 'OTKAZANA'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3"""

Q_STAR = f"""
SELECT c.region, p.category, d.year, SUM(f.revenue) AS revenue
{STAR_JOIN}
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3"""

Q_MV = """
SELECT region, category, year, SUM(revenue) AS revenue
FROM dw.mv_sales_month
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3"""


# ---------------------------------------------------------------------------
# Grafici
# ---------------------------------------------------------------------------

PALETTE = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]
BLUE_RAMP = ["#86b6ef", "#2a78d6", "#104281"]  # ordinalna skala (p50 < p95 < p99)
INK, INK2, MUTED, GRID, AXIS, SURFACE = "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7", "#fcfcfb"


def style_axes(ax, title, ylabel=None, xlabel=None, grid_axis="y"):
    ax.set_facecolor(SURFACE)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(AXIS)
    ax.tick_params(colors=MUTED, labelcolor=INK2)
    (ax.yaxis if grid_axis == "y" else ax.xaxis).grid(True, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.set_title(title, color=INK, fontsize=11, loc="left", pad=10)
    if ylabel:
        ax.set_ylabel(ylabel, color=INK2)
    if xlabel:
        ax.set_xlabel(xlabel, color=INK2)


def make_charts(out_dir, oltp_run, interference, cmp_rows, olap_times):
    if not HAVE_MPL:
        info("matplotlib nije instaliran - grafici se preskaču (pip install matplotlib).")
        return []
    files = []

    # 1) Latencija OLTP transakcija po tipu
    fig, ax = plt.subplots(figsize=(9, 4.6), facecolor=SURFACE)
    types = list(oltp_run["by_type"].keys())
    width = 0.22
    for pi, p in enumerate(("p50", "p95", "p99")):
        vals = [oltp_run["by_type"][t][p] for t in types]
        xs = [i + (pi - 1) * width for i in range(len(types))]
        bars = ax.bar(xs, vals, width * 0.9, color=BLUE_RAMP[pi], label=p)
        for b, v in zip(bars, vals):
            ax.text(b.get_x() + b.get_width() / 2, v, f"{v:.1f}", ha="center", va="bottom", color=INK2, fontsize=8)
    ax.set_xticks(range(len(types)))
    ax.set_xticklabels([TX_LABELS.get(t, t) for t in types])
    style_axes(ax, f"OLTP: latencija transakcija (TPS = {oltp_run['tps']:,.0f})", "milisekunde")
    ax.legend(frameon=False, labelcolor=INK2, fontsize=9)
    fig.tight_layout()
    f = out_dir / "grafik_oltp_latencija.png"
    fig.savefig(f, dpi=150, facecolor=SURFACE)
    plt.close(fig)
    files.append(f)

    # 2) Interferencija: TPS i p95 (dve mere -> dva grafika jedan pored drugog)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4.4), facecolor=SURFACE)
    labels = ["samo OLTP", "OLTP + OLAP"]
    tps = [interference["alone"]["tps"], interference["mixed"]["tps"]]
    p95 = [interference["alone"]["p95"], interference["mixed"]["p95"]]
    for ax, vals, title, unit, fmt in ((ax1, tps, "Propusna moć (TPS)", "transakcija / s", "{:,.0f}"),
                                       (ax2, p95, "Latencija p95", "milisekunde", "{:.1f} ms")):
        bars = ax.bar(range(2), vals, 0.45, color=[PALETTE[0], PALETTE[1]])
        for b, v in zip(bars, vals):
            ax.text(b.get_x() + b.get_width() / 2, v, fmt.format(v), ha="center", va="bottom", color=INK2,
                    fontsize=9)
        ax.set_xticks(range(2))
        ax.set_xticklabels(labels)
        ax.set_ylim(0, max(vals) * 1.2)
        style_axes(ax, title, unit)
    fig.suptitle("Uticaj analitičkih upita na OLTP opterećenje (isti server)", color=INK, fontsize=12, x=0.02,
                 ha="left")
    fig.tight_layout()
    f = out_dir / "grafik_interferencija.png"
    fig.savefig(f, dpi=150, facecolor=SURFACE)
    plt.close(fig)
    files.append(f)

    # 3) Isto pitanje: 3NF vs star vs mat. pogled
    fig, ax = plt.subplots(figsize=(8, 4.4), facecolor=SURFACE)
    names = [r[0] for r in cmp_rows]
    vals = [r[1] for r in cmp_rows]
    bars = ax.bar(range(len(vals)), vals, 0.45, color=PALETTE[0])
    for b, v in zip(bars, vals):
        ax.text(b.get_x() + b.get_width() / 2, v, f"{v:.1f} ms", ha="center", va="bottom", color=INK2, fontsize=9)
    ax.set_xticks(range(len(vals)))
    ax.set_xticklabels(names, fontsize=9)
    ax.set_ylim(0, max(vals) * 1.2)
    style_axes(ax, "Prihod po regionu, kategoriji i godini - isto pitanje, različiti modeli", "milisekunde")
    fig.tight_layout()
    f = out_dir / "grafik_3nf_vs_star.png"
    fig.savefig(f, dpi=150, facecolor=SURFACE)
    plt.close(fig)
    files.append(f)

    # 4) OLAP upiti nad star šemom
    fig, ax = plt.subplots(figsize=(9, 4.8), facecolor=SURFACE)
    names = [n for n, _ in olap_times][::-1]
    vals = [v for _, v in olap_times][::-1]
    bars = ax.barh(range(len(vals)), vals, 0.5, color=PALETTE[0])
    for b, v in zip(bars, vals):
        ax.text(v, b.get_y() + b.get_height() / 2, f" {v:.0f} ms", va="center", color=INK2, fontsize=8)
    ax.set_yticks(range(len(vals)))
    ax.set_yticklabels(names, fontsize=8)
    ax.set_xlim(0, max(vals) * 1.2)
    style_axes(ax, "OLAP upiti nad zvezdastom šemom (medijana)", xlabel="milisekunde", grid_axis="x")
    fig.tight_layout()
    f = out_dir / "grafik_olap_upiti.png"
    fig.savefig(f, dpi=150, facecolor=SURFACE)
    plt.close(fig)
    files.append(f)
    return files


# ---------------------------------------------------------------------------
# Glavni tok
# ---------------------------------------------------------------------------

def parse_args():
    here = Path(__file__).resolve().parent
    p = argparse.ArgumentParser(description="Demonstracija OLTP i OLAP sistema (PostgreSQL)")
    p.add_argument("--host", default=os.environ.get("PGHOST", "localhost"))
    p.add_argument("--port", type=int, default=int(os.environ.get("PGPORT", 5432)))
    p.add_argument("--user", default=os.environ.get("PGUSER", "postgres"))
    p.add_argument("--password", default=os.environ.get("PGPASSWORD"))
    p.add_argument("--db", default="oltp_olap_demo", help="ime baze koja se kreira")
    p.add_argument("--scale", type=float, default=1.0,
                   help="faktor veličine (1.0 = 20k kupaca, 2k proizvoda, 200k narudžbina)")
    p.add_argument("--threads", type=int, default=4, help="broj OLTP klijenata (niti)")
    p.add_argument("--olap-threads", type=int, default=2, help="broj OLAP niti u testu interferencije")
    p.add_argument("--duration", type=int, default=15, help="trajanje svakog OLTP testa u sekundama")
    p.add_argument("--runs", type=int, default=3, help="broj merenja po OLAP upitu (posle zagrevanja)")
    p.add_argument("--out", default=str(here / "rezultati"), help="direktorijum za rezultate")
    p.add_argument("--keep", action="store_true", help="ne brisati bazu na kraju")
    return p.parse_args()


def main():
    args = parse_args()
    started = datetime.now()
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    md = ["# OLTP i OLAP sistemi - rezultati\n"]

    # ------------------------------------------------------------------ 1
    korak(1, "Priprema baze")
    try:
        admin = connect(args, dbname="postgres")
    except psycopg2.OperationalError as e:
        sys.exit(f"Ne mogu da se povežem na PostgreSQL ({args.host}:{args.port}): {e}")
    pg_version = query(admin, "SHOW server_version")[0][0]
    execute(admin, f"DROP DATABASE IF EXISTS {args.db} WITH (FORCE)")
    execute(admin, f"CREATE DATABASE {args.db}")
    admin.close()
    conn = connect(args)
    info(f"PostgreSQL {pg_version}; kreirana baza '{args.db}'.")
    info(f"OLTP klijenata: {args.threads}, OLAP niti (test interferencije): {args.olap_threads}, "
         f"trajanje testa: {args.duration} s")
    md.append(f"Datum: {started:%d.%m.%Y %H:%M} · PostgreSQL {pg_version} · scale = {args.scale} · "
              f"{args.threads} OLTP klijenata · {args.duration} s po testu\n")

    # ------------------------------------------------------------------ 2
    korak(2, "OLTP šema u 3NF + istorijski podaci")
    execute(conn, OLTP_DDL)
    info("Tabele: region -> city -> customer; category -> product; orders -> order_item, payment")
    info("Ograničenja: PK, FK, UNIQUE, CHECK (stock >= 0, quantity > 0, status IN (...))")
    n_customers, n_products, n_orders = load_oltp_data(conn, args.scale)
    counts = query(conn, """
        SELECT relname, n_live_tup, pg_total_relation_size(relid)
        FROM pg_stat_user_tables WHERE schemaname = 'oltp' ORDER BY pg_total_relation_size(relid) DESC""")
    print()
    print_table(["tabela", "redova", "veličina (sa indeksima)"],
                [(f"oltp.{n}", f"{c:,}", fmt_bytes(s)) for n, c, s in counts])
    md.append("## 1. OLTP šema (3NF)\n")
    md.append(md_table(["tabela", "redova", "veličina (sa indeksima)"],
                       [(f"oltp.{n}", f"{c:,}", fmt_bytes(s)) for n, c, s in counts]))

    # ------------------------------------------------------------------ 3
    korak(3, "OLTP radno opterećenje (kratke konkurentne transakcije)")
    info("Mešavina: 50% nova narudžbina, 30% provera statusa (čitanje po PK), 20% isporuka (UPDATE).")
    info("Nova narudžbina: SELECT ... FOR UPDATE (zaliha) -> INSERT orders/order_item/payment -> UPDATE stock.")
    stock_before = query(conn, "SELECT SUM(stock) FROM oltp.product")[0][0]
    max_before = query(conn, "SELECT max(order_id) FROM oltp.orders")[0][0]
    ctx = {"n_customers": n_customers, "n_products": n_products, "max_order_id": max_before}
    oltp_run = run_workload(args, ctx, args.duration)
    info(f"Trajanje: {oltp_run['elapsed']:.1f} s; potvrđeno (COMMIT): {oltp_run['commits']:,}; "
         f"poništeno (ROLLBACK - nedovoljna zaliha): {oltp_run['rollbacks']:,}; deadlock/greške: {oltp_run['errors']}")
    info(f"Propusna moć: {oltp_run['tps']:,.0f} TPS; latencija p50 {oltp_run['p50']:.2f} ms, "
         f"p95 {oltp_run['p95']:.2f} ms, p99 {oltp_run['p99']:.2f} ms")
    lat_rows = [(k, f"{v['n']:,}", f"{v['avg']:.2f}", f"{v['p50']:.2f}", f"{v['p95']:.2f}", f"{v['p99']:.2f}")
                for k, v in oltp_run["by_type"].items()]
    print()
    print_table(["transakcija", "broj", "prosek ms", "p50 ms", "p95 ms", "p99 ms"], lat_rows)

    # ACID provera
    print()
    info("Provera ACID svojstava:")
    stock_after = query(conn, "SELECT SUM(stock) FROM oltp.product")[0][0]
    sold = query(conn, "SELECT COALESCE(SUM(quantity), 0) FROM oltp.order_item WHERE order_id > %s",
                 (max_before,))[0][0]
    negative = query(conn, "SELECT count(*) FROM oltp.product WHERE stock < 0")[0][0]
    orphan = query(conn, """SELECT count(*) FROM oltp.orders o WHERE o.order_id > %s AND
                            (NOT EXISTS (SELECT 1 FROM oltp.order_item i WHERE i.order_id = o.order_id)
                             OR NOT EXISTS (SELECT 1 FROM oltp.payment p WHERE p.order_id = o.order_id))""",
                   (max_before,))[0][0]
    bad_total = query(conn, """SELECT count(*) FROM oltp.orders o WHERE o.order_id > %s AND o.total <>
                               (SELECT SUM(quantity * unit_price) FROM oltp.order_item i
                                WHERE i.order_id = o.order_id)""", (max_before,))[0][0]
    acid = [
        ("Konzistentnost: početna zaliha - trenutna = prodato", f"{stock_before - stock_after:,} = {sold:,}",
         stock_before - stock_after == sold),
        ("Konzistentnost: nijedan proizvod nema negativnu zalihu", f"{negative} proizvoda", negative == 0),
        ("Atomičnost: nema narudžbine bez stavki ili bez uplate", f"{orphan} narudžbina", orphan == 0),
        ("Konzistentnost: orders.total = suma stavki", f"{bad_total} odstupanja", bad_total == 0),
        ("Izolacija: FOR UPDATE + fiksni redosled zaključavanja", f"{oltp_run['errors']} deadlock-a",
         oltp_run["errors"] == 0),
    ]
    for name, val, ok in acid:
        info(f"  [{'OK' if ok else 'GREŠKA'}] {name}: {val}")
    info("  Trajnost (durability): COMMIT se potvrđuje tek kada je WAL zapis upisan na disk (synchronous_commit).")
    md.append("\n## 2. OLTP opterećenje\n")
    md.append(f"- Potvrđenih transakcija: **{oltp_run['commits']:,}** za {oltp_run['elapsed']:.1f} s "
              f"-> **{oltp_run['tps']:,.0f} TPS**")
    md.append(f"- Poništenih (ROLLBACK zbog nedovoljne zalihe): {oltp_run['rollbacks']:,}; deadlock: "
              f"{oltp_run['errors']}")
    md.append(f"- Latencija (sve transakcije): p50 {oltp_run['p50']:.2f} ms, p95 {oltp_run['p95']:.2f} ms, "
              f"p99 {oltp_run['p99']:.2f} ms\n")
    md.append(md_table(["transakcija", "broj", "prosek ms", "p50 ms", "p95 ms", "p99 ms"], lat_rows))
    md.append("\n### Provera ACID svojstava\n")
    md.append(md_table(["provera", "vrednost", "rezultat"], [(n, v, "OK" if ok else "GREŠKA") for n, v, ok in acid]))

    # ------------------------------------------------------------------ 4
    korak(4, "Obrasci pristupa podacima: OLTP upit vs analitički upit")
    sample_id = max_before // 2
    access = [
        ("OLTP: narudžbina po PK", f"""SELECT o.order_id, o.status, o.total, c.first_name, c.last_name
                                     FROM oltp.orders o JOIN oltp.customer c ON c.customer_id = o.customer_id
                                     WHERE o.order_id = {sample_id}"""),
        ("OLTP: zaliha proizvoda", "SELECT price, stock FROM oltp.product WHERE product_id = 42"),
        ("OLTP: poslednjih 5 narudžbina kupca", """SELECT order_id, order_date, total FROM oltp.orders
                                                 WHERE customer_id = 777 ORDER BY order_date DESC LIMIT 5"""),
        ("OLAP: prihod po regionu/kategoriji/god.", Q_3NF),
    ]
    access_rows = []
    metrics = []
    for name, sql in access:
        m = plan_metrics(conn, sql)
        metrics.append(m)
        scans = ", ".join(sorted(set(n.split(" (")[0] for n in m["nodes"])))
        access_rows.append((name, f"{m['time']:.2f} ms", f"{m['scanned']:,}", f"{m['blocks']:,}",
                            f"{m['rows_out']:,}", scans))
    print_table(["upit", "vreme", "pročitano redova", "blokova (8kB)", "vraćeno", "tip pristupa"], access_rows)
    oltp_access, olap_access = metrics[0], metrics[-1]
    info("OLTP upiti čitaju nekoliko redova preko indeksa; analitički upit skenira cele tabele i agregira.")
    md.append("\n## 3. Obrasci pristupa podacima\n")
    md.append(md_table(["upit", "vreme", "pročitano redova", "blokova (8kB)", "vraćeno", "tip pristupa"],
                       access_rows))

    # ------------------------------------------------------------------ 5
    korak(5, "ETL: izgradnja skladišta podataka (star schema)")
    execute(conn, DW_DDL)
    info("dw.fact_sales (granularnost: stavka narudžbine) + dimenzije dim_date, dim_customer, dim_product")
    etl_rows = []
    for name, sql in ETL_DIMENSIONS:
        with conn.cursor() as cur:
            t0 = time.perf_counter()
            cur.execute(sql)
            etl_rows.append((f"Load dw.{name}", f"{cur.rowcount:,}", f"{time.perf_counter() - t0:.2f} s"))
    dt, n, od, do = etl_fact(conn, "FULL")
    etl_rows.append((f"Load dw.fact_sales (order_id {od + 1}..{do})", f"{n:,}", f"{dt:.2f} s"))
    t0 = time.perf_counter()
    execute(conn, "CREATE INDEX idx_fact_date_brin ON dw.fact_sales USING brin (date_key)")
    execute(conn, "CREATE INDEX idx_fact_product ON dw.fact_sales (product_key)")
    execute(conn, "ANALYZE dw.fact_sales, dw.dim_date, dw.dim_customer, dw.dim_product")
    etl_rows.append(("Indeksi (BRIN date_key, B-tree product_key) + ANALYZE", "", f"{time.perf_counter() - t0:.2f} s"))
    t0 = time.perf_counter()
    execute(conn, MV_DDL)
    mv_rows = query(conn, "SELECT count(*) FROM dw.mv_sales_month")[0][0]
    etl_rows.append(("Materijalizovani pogled dw.mv_sales_month", f"{mv_rows:,}", f"{time.perf_counter() - t0:.2f} s"))
    print_table(["ETL korak", "redova", "trajanje"], etl_rows)
    brin_size, fact_size = query(conn, """SELECT pg_relation_size('dw.idx_fact_date_brin'),
                                                pg_relation_size('dw.fact_sales')""")[0]
    btree_size = query(conn, "SELECT pg_relation_size('dw.idx_fact_product')")[0][0]
    info(f"fact_sales: {fmt_bytes(fact_size)}; BRIN indeks: {fmt_bytes(brin_size)} "
         f"(B-tree nad product_key: {fmt_bytes(btree_size)}) - BRIN je mali jer su podaci učitani po datumu.")
    md.append("\n## 4. ETL i skladište podataka\n")
    md.append(md_table(["ETL korak", "redova", "trajanje"], etl_rows))
    md.append(f"\nVeličina `fact_sales`: {fmt_bytes(fact_size)}; BRIN indeks nad `date_key`: {fmt_bytes(brin_size)}; "
              f"B-tree nad `product_key`: {fmt_bytes(btree_size)}.\n")

    # ------------------------------------------------------------------ 6
    korak(6, "OLAP upiti nad zvezdastom šemom")
    olap_times = []
    olap_txt = []
    md.append("## 5. OLAP upiti\n")
    for name, desc, sql, sample in OLAP_QUERIES:
        ms, rows = timed_query(conn, sql, args.runs)
        olap_times.append((name, ms))
        print()
        info(f"{name}  [{ms:.1f} ms, {len(rows)} redova]")
        info(f"  {desc}")
        shown = sample(rows)
        for r in shown:
            info("    " + " | ".join(fmt_cell(v) for v in r))
        olap_txt.append(f"-- {name} ({ms:.1f} ms)\n{' '.join(sql.split())}\n" +
                        "\n".join(" | ".join("*" if v is None else str(v) for v in r) for r in rows) + "\n")
        md.append(f"**{name}** - {desc} ({ms:.1f} ms, {len(rows)} redova)\n")
        md.append("```sql\n" + "\n".join(line.strip() for line in sql.strip().splitlines()) + "\n```\n")
    (out_dir / "olap_rezultati.txt").write_text("\n".join(olap_txt), encoding="utf-8")
    info("(`*` označava međuzbir/ukupno u ROLLUP/CUBE; kompletni rezultati: olap_rezultati.txt)")

    print()
    info("Isto poslovno pitanje - 'prihod po regionu, kategoriji i godini':")
    cmp_rows = []
    results = {}
    for label, sql, joins in (("3NF (OLTP šema)", Q_3NF, 6), ("Star schema", Q_STAR, 3),
                              ("Mat. pogled", Q_MV, 0)):
        ms, rows = timed_query(conn, sql, args.runs)
        results[label] = [(r[0], r[1], int(r[2]), round(float(r[3]), 2)) for r in rows]
        cmp_rows.append((label, ms, joins, len(rows)))
    same = results["3NF (OLTP šema)"] == results["Star schema"] == results["Mat. pogled"]
    print_table(["model", "vreme", "spajanja", "redova"],
                [(n, f"{ms:.1f} ms", j, r) for n, ms, j, r in cmp_rows])
    info(f"Rezultati sva tri upita su {'IDENTIČNI' if same else 'RAZLIČITI (!)'}.")
    md.append("### Isto pitanje nad različitim modelima\n")
    md.append(md_table(["model", "vreme (medijana)", "broj spajanja", "redova"],
                       [(n, f"{ms:.1f} ms", j, r) for n, ms, j, r in cmp_rows]))
    md.append(f"\nRezultati su {'identični' if same else 'RAZLIČITI'}.\n")

    # ------------------------------------------------------------------ 7
    korak(7, "Interferencija: OLTP sam vs OLTP + OLAP na istom sistemu")
    ctx["max_order_id"] = query(conn, "SELECT max(order_id) FROM oltp.orders")[0][0]
    info(f"a) {args.threads} OLTP klijenata, {args.duration} s ...")
    alone = run_workload(args, ctx, args.duration, seed_base=200)
    info(f"   {alone['tps']:,.0f} TPS, p95 {alone['p95']:.2f} ms")
    info(f"b) {args.threads} OLTP klijenata + {args.olap_threads} niti koje neprestano izvršavaju analitički "
         f"upit nad OLTP tabelama ...")
    mixed = run_workload(args, ctx, args.duration, olap_sql=Q_3NF, seed_base=200)
    info(f"   {mixed['tps']:,.0f} TPS, p95 {mixed['p95']:.2f} ms (izvršeno {mixed['olap_queries']} OLAP upita)")
    drop = 100 * (1 - mixed["tps"] / alone["tps"]) if alone["tps"] else 0
    lat_up = mixed["p95"] / alone["p95"] if alone["p95"] else 0
    info(f"Pad propusne moći: {drop:.0f}%; p95 latencija ×{lat_up:.1f}")
    info("Zaključak: analitika na produkcionoj OLTP bazi troši CPU, I/O i keš koji trebaju transakcijama -")
    info("zato se OLAP izdvaja u posebno skladište podataka (DW) koje se puni ETL procesom.")
    interference = {"alone": alone, "mixed": mixed}
    md.append("\n## 6. Interferencija OLTP i OLAP opterećenja\n")
    md.append(md_table(["scenario", "TPS", "p50 ms", "p95 ms", "p99 ms", "OLAP upita"],
                       [("samo OLTP", f"{alone['tps']:,.0f}", f"{alone['p50']:.2f}", f"{alone['p95']:.2f}",
                         f"{alone['p99']:.2f}", "-"),
                        ("OLTP + OLAP", f"{mixed['tps']:,.0f}", f"{mixed['p50']:.2f}", f"{mixed['p95']:.2f}",
                         f"{mixed['p99']:.2f}", mixed["olap_queries"])]))
    md.append(f"\nPad propusne moći: **{drop:.0f}%**, p95 latencija **×{lat_up:.1f}**.\n")

    # ------------------------------------------------------------------ 8
    korak(8, "Inkrementalni ETL i kašnjenje podataka u skladištu")
    freshness_sql = {
        "OLTP": """SELECT count(DISTINCT o.order_id), COALESCE(SUM(i.quantity * i.unit_price), 0)
                   FROM oltp.orders o JOIN oltp.order_item i ON i.order_id = o.order_id
                   WHERE o.status <> 'OTKAZANA' AND o.order_date >= current_date""",
        "DW": """SELECT count(DISTINCT f.order_id), COALESCE(SUM(f.revenue), 0)
                 FROM dw.fact_sales f WHERE f.date_key >= to_char(current_date, 'YYYYMMDD')::int""",
    }
    before = {k: query(conn, v)[0] for k, v in freshness_sql.items()}
    info(f"Današnje narudžbine - OLTP: {before['OLTP'][0]:,} ({fmt_num(before['OLTP'][1])} RSD), "
         f"DW: {before['DW'][0]:,} ({fmt_num(before['DW'][1])} RSD)")
    info("DW 'kasni' - ne vidi narudžbine nastale posle poslednjeg ETL-a.")
    dt, n, od, do = etl_fact(conn, "INCREMENTAL")
    t0 = time.perf_counter()
    execute(conn, "REFRESH MATERIALIZED VIEW dw.mv_sales_month")
    refresh_dt = time.perf_counter() - t0
    after = {k: query(conn, v)[0] for k, v in freshness_sql.items()}
    info(f"Inkrementalni ETL (watermark order_id > {od}): {n:,} novih redova činjenica za {dt:.2f} s; "
         f"REFRESH MATERIALIZED VIEW: {refresh_dt:.2f} s")
    info(f"Posle ETL-a - OLTP: {after['OLTP'][0]:,} ({fmt_num(after['OLTP'][1])} RSD), "
         f"DW: {after['DW'][0]:,} ({fmt_num(after['DW'][1])} RSD)")
    synced = after["OLTP"] == after["DW"]
    info("Skladište je sinhronizovano sa OLTP sistemom." if synced else "UPOZORENJE: DW i OLTP se razlikuju!")
    etl_log = query(conn, "SELECT load_id, load_type, from_order, to_order, rows_loaded FROM dw.etl_log ORDER BY 1")
    md.append("## 7. Inkrementalni ETL\n")
    md.append(md_table(["", "OLTP narudžbina danas", "OLTP prihod danas", "DW narudžbina danas", "DW prihod danas"],
                       [("pre ETL-a", f"{before['OLTP'][0]:,}", fmt_num(before["OLTP"][1]), f"{before['DW'][0]:,}",
                         fmt_num(before["DW"][1])),
                        ("posle ETL-a", f"{after['OLTP'][0]:,}", fmt_num(after["OLTP"][1]), f"{after['DW'][0]:,}",
                         fmt_num(after["DW"][1]))]))
    md.append("\n" + md_table(["load", "tip", "od order_id", "do order_id", "redova"],
                              [(a, b, c, d, f"{e:,}") for a, b, c, d, e in etl_log]))
    md.append(f"\nInkrementalni ETL: {dt:.2f} s; osvežavanje materijalizovanog pogleda: {refresh_dt:.2f} s.\n")

    # ------------------------------------------------------------------ 9
    korak(9, "Zbirni rezultati: OLTP vs OLAP")
    olap_avg = statistics.mean(ms for _, ms in olap_times)
    summary = [
        ("Tipična operacija", f"INSERT/UPDATE/SELECT - čita {oltp_access['scanned']:,} red(a)",
         f"agregacija - čita {olap_access['scanned']:,} redova"),
        ("Broj operacija", f"{oltp_run['tps']:,.0f} transakcija/s", f"{1000 / olap_avg:.1f} upita/s (1 nit)"),
        ("Latencija", f"p50 {oltp_run['p50']:.2f} ms", f"prosek {olap_avg:.0f} ms po upitu"),
        ("Model podataka", "3NF (normalizovan)", "star schema (denormalizovan)"),
        ("Blokova (8 kB) po upitu", f"{oltp_access['blocks']:,}", f"{olap_access['blocks']:,}"),
        ("Pristup podacima", "Index Scan po ključu", "Seq Scan + Hash Join + agregacija"),
        ("Svežina podataka", "trenutna", "do poslednjeg ETL-a"),
    ]
    print_table(["osobina", "OLTP (izmereno)", "OLAP (izmereno)"], summary)
    md.append("## 8. OLTP vs OLAP - sažetak merenja\n")
    md.append(md_table(["osobina", "OLTP", "OLAP"], summary))

    with open(out_dir / "rezultati.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["test", "metrika", "vrednost"])
        for k, v in (("oltp", oltp_run), ("samo_oltp", alone), ("oltp_plus_olap", mixed)):
            for m in ("tps", "p50", "p95", "p99", "commits", "rollbacks", "errors", "olap_queries"):
                w.writerow([k, m, f"{v[m]:.3f}" if isinstance(v[m], float) else v[m]])
        for name, ms in olap_times:
            w.writerow(["olap_upit", name, f"{ms:.3f}"])
        for name, ms, _, _ in cmp_rows:
            w.writerow(["isto_pitanje", name, f"{ms:.3f}"])

    charts = make_charts(out_dir, oltp_run, interference, [(n, ms) for n, ms, _, _ in cmp_rows], olap_times)
    md.append("\n## 9. Fajlovi\n")
    md.append("- `rezultati.csv` - sirovi rezultati\n- `olap_rezultati.txt` - kompletni rezultati OLAP upita")
    for c in charts:
        md.append(f"- `{c.name}`\n\n![{c.stem}]({c.name})")
    report = out_dir / "izvestaj.md"
    report.write_text("\n".join(md) + "\n", encoding="utf-8")

    print()
    info(f"Rezultati sačuvani u: {out_dir}")
    for f in [report, out_dir / "rezultati.csv", out_dir / "olap_rezultati.txt", *charts]:
        info(f"  - {f.name}")
    conn.close()
    if not args.keep:
        admin = connect(args, dbname="postgres")
        execute(admin, f"DROP DATABASE IF EXISTS {args.db} WITH (FORCE)")
        admin.close()
        info(f"Baza '{args.db}' obrisana (za zadržavanje koristite --keep).")
    else:
        info(f"Baza '{args.db}' je zadržana (šeme oltp i dw).")
    info(f"Ukupno trajanje: {(datetime.now() - started).total_seconds():.0f} s")


if __name__ == "__main__":
    main()
