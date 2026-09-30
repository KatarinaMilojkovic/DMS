#!/usr/bin/env python3
"""
Seminarski rad: Optimizacija upita primenom TPC-like benchmarking pristupa
==========================================================================

Skripta demonstrira kompletan ciklus benchmark-vođene optimizacije upita
nad PostgreSQL bazom, po uzoru na TPC-H benchmark:

  KORAK 1  Priprema okruženja (baza, informacije o serveru)
  KORAK 2  Kreiranje TPC-H-like šeme (8 tabela)
  KORAK 3  Generisanje podataka za zadati faktor skaliranja (SF)
  KORAK 4  Faza 0 - Baseline merenje (bez indeksa, bez statistike)
  KORAK 5  Faza 1 - ANALYZE (statistika za optimizator)
  KORAK 6  Faza 2 - Indeksi (PK, FK, filter, pokrivajući, kompozitni)
  KORAK 7  Faza 3 - Prepisivanje upita (sargable uslovi, dekorelacija)
  KORAK 8  Faza 4 - Podešavanje konfiguracije (work_mem, paralelizam, ...)
  KORAK 9  Faza 5 - Materijalizovani pogled (pre-agregacija)
  KORAK 10 Throughput test (više paralelnih tokova upita)
  KORAK 11 Zbirni rezultati, grafici i izveštaj

Napomena: ovo NIJE zvanični TPC-H benchmark (koji zahteva dbgen/qgen alate
i reviziju rezultata), već pojednostavljena "TPC-like" varijanta: šema,
distribucije podataka i upiti su izvedeni iz TPC-H specifikacije.

Pokretanje (primer):
    python tpc_optimizacija.py --host localhost --user postgres --password postgres --scale 0.1
"""

import argparse
import csv
import hashlib
import json
import math
import os
import random
import re
import statistics
import sys
import threading
import time
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
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
# Pomoćne funkcije za ispis
# ---------------------------------------------------------------------------

def korak(broj, naslov):
    print()
    print("=" * 78)
    print(f"=== KORAK {broj}: {naslov}")
    print("=" * 78)


def info(tekst=""):
    print(f"    {tekst}")


def fmt_ms(ms, timeout=False):
    if ms is None:
        return "-"
    prefix = ">=" if timeout else ""
    if ms >= 10000:
        return f"{prefix}{ms / 1000:.1f} s"
    return f"{prefix}{ms:.1f} ms"


def fmt_bytes(n):
    for unit in ("B", "kB", "MB", "GB"):
        if abs(n) < 1024 or unit == "GB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024
    return f"{n:.1f} GB"


def print_table(headers, rows):
    """Ispisuje tabelu poravnatih kolona u konzoli."""
    widths = [len(str(h)) for h in headers]
    for r in rows:
        for i, v in enumerate(r):
            widths[i] = max(widths[i], len(str(v)))
    line = "  ".join(str(h).ljust(widths[i]) for i, h in enumerate(headers))
    info(line)
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
# Rad sa bazom
# ---------------------------------------------------------------------------

def connect(args, dbname=None, autocommit=True):
    conn = psycopg2.connect(host=args.host, port=args.port, user=args.user,
                            password=args.password, dbname=dbname or args.db)
    conn.autocommit = autocommit
    return conn


def exec_timed(conn, sql):
    """Izvršava SQL naredbu i vraća trajanje u sekundama."""
    t0 = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(sql)
    return time.perf_counter() - t0


def scalar(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()[0]


# ---------------------------------------------------------------------------
# KORAK 2: TPC-H-like šema
# ---------------------------------------------------------------------------
# Tabele se kreiraju BEZ primarnih ključeva i indeksa, a autovacuum je
# isključen kako bi baseline zaista bio "neoptimizovano" stanje (optimizator
# nema statistiku o raspodeli podataka).

NOAV = "WITH (autovacuum_enabled = false)"

SCHEMA_DDL = f"""
CREATE TABLE region (
    r_regionkey  integer      NOT NULL,
    r_name       char(25)     NOT NULL,
    r_comment    varchar(152)
) {NOAV};

CREATE TABLE nation (
    n_nationkey  integer      NOT NULL,
    n_name       char(25)     NOT NULL,
    n_regionkey  integer      NOT NULL,
    n_comment    varchar(152)
) {NOAV};

CREATE TABLE supplier (
    s_suppkey    integer        NOT NULL,
    s_name       char(25)       NOT NULL,
    s_address    varchar(40)    NOT NULL,
    s_nationkey  integer        NOT NULL,
    s_phone      char(15)       NOT NULL,
    s_acctbal    numeric(15,2)  NOT NULL,
    s_comment    varchar(101)   NOT NULL
) {NOAV};

CREATE TABLE part (
    p_partkey     integer        NOT NULL,
    p_name        varchar(55)    NOT NULL,
    p_mfgr        char(25)       NOT NULL,
    p_brand       char(10)       NOT NULL,
    p_type        varchar(25)    NOT NULL,
    p_size        integer        NOT NULL,
    p_container   char(10)       NOT NULL,
    p_retailprice numeric(15,2)  NOT NULL,
    p_comment     varchar(23)    NOT NULL
) {NOAV};

CREATE TABLE partsupp (
    ps_partkey    integer        NOT NULL,
    ps_suppkey    integer        NOT NULL,
    ps_availqty   integer        NOT NULL,
    ps_supplycost numeric(15,2)  NOT NULL,
    ps_comment    varchar(199)   NOT NULL
) {NOAV};

CREATE TABLE customer (
    c_custkey     integer        NOT NULL,
    c_name        varchar(25)    NOT NULL,
    c_address     varchar(40)    NOT NULL,
    c_nationkey   integer        NOT NULL,
    c_phone       char(15)       NOT NULL,
    c_acctbal     numeric(15,2)  NOT NULL,
    c_mktsegment  char(10)       NOT NULL,
    c_comment     varchar(117)   NOT NULL
) {NOAV};

CREATE TABLE orders (
    o_orderkey      integer        NOT NULL,
    o_custkey       integer        NOT NULL,
    o_orderstatus   char(1)        NOT NULL,
    o_totalprice    numeric(15,2)  NOT NULL,
    o_orderdate     date           NOT NULL,
    o_orderpriority char(15)       NOT NULL,
    o_clerk         char(15)       NOT NULL,
    o_shippriority  integer        NOT NULL,
    o_comment       varchar(79)    NOT NULL
) {NOAV};

CREATE TABLE lineitem (
    l_orderkey      integer        NOT NULL,
    l_partkey       integer        NOT NULL,
    l_suppkey       integer        NOT NULL,
    l_linenumber    integer        NOT NULL,
    l_quantity      numeric(15,2)  NOT NULL,
    l_extendedprice numeric(15,2)  NOT NULL,
    l_discount      numeric(15,2)  NOT NULL,
    l_tax           numeric(15,2)  NOT NULL,
    l_returnflag    char(1)        NOT NULL,
    l_linestatus    char(1)        NOT NULL,
    l_shipdate      date           NOT NULL,
    l_commitdate    date           NOT NULL,
    l_receiptdate   date           NOT NULL,
    l_shipinstruct  char(25)       NOT NULL,
    l_shipmode      char(10)       NOT NULL,
    l_comment       varchar(44)    NOT NULL
) {NOAV};
"""

TABLES = ["region", "nation", "supplier", "part", "partsupp", "customer", "orders", "lineitem"]


# ---------------------------------------------------------------------------
# KORAK 3: Generisanje podataka (u samoj bazi, pomoću generate_series)
# ---------------------------------------------------------------------------
# Kardinalnosti prate TPC-H: SUPPLIER = 10.000*SF, PART = 200.000*SF,
# PARTSUPP = 4*PART, CUSTOMER = 150.000*SF, ORDERS = 1.500.000*SF,
# LINEITEM ~ 4*ORDERS (1-7 stavki po narudžbini).
# setseed() obezbeđuje reproduktivnost - isti SF daje iste podatke.

RETAIL_PRICE = "((90000 + ((({k}) / 10) % 20001) + 100 * (({k}) % 1000)) / 100.0)"


def data_generation_steps(sf):
    S = max(100, int(10000 * sf))
    P = max(2000, int(200000 * sf))
    C = max(1500, int(150000 * sf))
    O = max(15000, int(1500000 * sf))
    clerks = max(10, int(1000 * sf))
    rp_part = RETAIL_PRICE.format(k="p")
    rp_line = RETAIL_PRICE.format(k="l_partkey")
    supp = "((({pk}) + ({i}) * ({S} / 4 + (({pk}) - 1) / {S})) % {S}) + 1"

    steps = [
        ("region", """
            INSERT INTO region VALUES
              (0, 'AFRICA', 'lar deposits. blithely final packages cajole.'),
              (1, 'AMERICA', 'hs use ironic, even requests. s'),
              (2, 'ASIA', 'ges. thinly even pinto beans ca'),
              (3, 'EUROPE', 'ly final courts cajole furiously final excuse'),
              (4, 'MIDDLE EAST', 'uickly special accounts cajole carefully blithely')"""),
        ("nation", """
            INSERT INTO nation (n_nationkey, n_name, n_regionkey, n_comment)
            SELECT k, n, r, 'nation comment ' || k FROM (VALUES
              (0,'ALGERIA',0),(1,'ARGENTINA',1),(2,'BRAZIL',1),(3,'CANADA',1),(4,'EGYPT',4),
              (5,'ETHIOPIA',0),(6,'FRANCE',3),(7,'GERMANY',3),(8,'INDIA',2),(9,'INDONESIA',2),
              (10,'IRAN',4),(11,'IRAQ',4),(12,'JAPAN',2),(13,'JORDAN',4),(14,'KENYA',0),
              (15,'MOROCCO',0),(16,'MOZAMBIQUE',0),(17,'PERU',1),(18,'CHINA',2),(19,'ROMANIA',3),
              (20,'SAUDI ARABIA',4),(21,'VIETNAM',2),(22,'RUSSIA',3),(23,'UNITED KINGDOM',3),
              (24,'UNITED STATES',1)) AS v(k, n, r)"""),
        ("supplier", f"""
            INSERT INTO supplier
            SELECT s, 'Supplier#' || lpad(s::text, 9, '0'), md5(s::text),
                   floor(random() * 25)::int,
                   lpad(floor(random() * 1e10)::bigint::text, 15, '0'),
                   round((random() * 10998.99 - 999.99)::numeric, 2),
                   md5(random()::text)
            FROM generate_series(1, {S}) AS s"""),
        ("part", f"""
            INSERT INTO part
            SELECT p, 'part ' || md5(p::text), 'Manufacturer#' || m,
                   'Brand#' || m || (1 + floor(random() * 5)::int),
                   (ARRAY['STANDARD','SMALL','MEDIUM','LARGE','ECONOMY','PROMO'])[1 + floor(random() * 6)::int]
                   || ' ' || (ARRAY['ANODIZED','BURNISHED','PLATED','POLISHED','BRUSHED'])[1 + floor(random() * 5)::int]
                   || ' ' || (ARRAY['TIN','NICKEL','BRASS','STEEL','COPPER'])[1 + floor(random() * 5)::int],
                   1 + floor(random() * 50)::int,
                   (ARRAY['SM','LG','MED','JUMBO','WRAP'])[1 + floor(random() * 5)::int]
                   || ' ' || (ARRAY['CASE','BOX','BAG','JAR','PKG','PACK','CAN','DRUM'])[1 + floor(random() * 8)::int],
                   {rp_part}::numeric(15,2),
                   left(md5(random()::text), 20)
            FROM (SELECT p, 1 + floor(random() * 5)::int AS m
                  FROM generate_series(1, {P}) AS p OFFSET 0) AS g"""),
        ("partsupp", f"""
            INSERT INTO partsupp
            SELECT p, {supp.format(pk='p', i='i', S=S)},
                   1 + floor(random() * 9999)::int,
                   round((1 + random() * 999)::numeric, 2),
                   md5(random()::text) || md5(random()::text)
            FROM generate_series(1, {P}) AS p, generate_series(0, 3) AS i"""),
        ("customer", f"""
            INSERT INTO customer
            SELECT c, 'Customer#' || lpad(c::text, 9, '0'), md5(c::text),
                   floor(random() * 25)::int,
                   lpad(floor(random() * 1e10)::bigint::text, 15, '0'),
                   round((random() * 10998.99 - 999.99)::numeric, 2),
                   (ARRAY['AUTOMOBILE','BUILDING','FURNITURE','MACHINERY','HOUSEHOLD'])[1 + floor(random() * 5)::int],
                   md5(random()::text)
            FROM generate_series(1, {C}) AS c"""),
        ("orders (privremeno)", f"""
            CREATE TEMP TABLE tmp_orders AS
            SELECT o AS o_orderkey,
                   1 + floor(random() * {C})::int AS o_custkey,
                   DATE '1992-01-01' + floor(random() * 2406)::int AS o_orderdate,
                   (ARRAY['1-URGENT','2-HIGH','3-MEDIUM','4-NOT SPECIFIED','5-LOW'])[1 + floor(random() * 5)::int] AS o_orderpriority,
                   'Clerk#' || lpad((1 + floor(random() * {clerks}))::int::text, 9, '0') AS o_clerk,
                   0 AS o_shippriority,
                   left(md5(random()::text), 25 + floor(random() * 8)::int) AS o_comment
            FROM generate_series(1, {O}) AS o"""),
        ("lineitem", f"""
            INSERT INTO lineitem
            SELECT o_orderkey, l_partkey,
                   {supp.format(pk='l_partkey', i='supp_i', S=S)} AS l_suppkey,
                   l_linenumber, l_quantity,
                   (l_quantity * {rp_line})::numeric(15,2) AS l_extendedprice,
                   l_discount, l_tax,
                   CASE WHEN o_orderdate + ship_off + receipt_off <= DATE '1995-06-17'
                        THEN CASE WHEN rf < 0.5 THEN 'R' ELSE 'A' END ELSE 'N' END,
                   CASE WHEN o_orderdate + ship_off > DATE '1995-06-17' THEN 'O' ELSE 'F' END,
                   o_orderdate + ship_off,
                   o_orderdate + commit_off,
                   o_orderdate + ship_off + receipt_off,
                   (ARRAY['DELIVER IN PERSON','COLLECT COD','NONE','TAKE BACK RETURN'])[instr],
                   (ARRAY['REG AIR','AIR','RAIL','SHIP','TRUCK','MAIL','FOB'])[mode],
                   l_comment
            FROM (
                SELECT o.o_orderkey, o.o_orderdate, n AS l_linenumber,
                       1 + floor(random() * {P})::int AS l_partkey,
                       floor(random() * 4)::int AS supp_i,
                       (1 + floor(random() * 50))::numeric(15,2) AS l_quantity,
                       (floor(random() * 11) / 100)::numeric(15,2) AS l_discount,
                       (floor(random() * 9) / 100)::numeric(15,2) AS l_tax,
                       1 + floor(random() * 121)::int AS ship_off,
                       30 + floor(random() * 61)::int AS commit_off,
                       1 + floor(random() * 30)::int AS receipt_off,
                       random() AS rf,
                       1 + floor(random() * 4)::int AS instr,
                       1 + floor(random() * 7)::int AS mode,
                       left(md5(random()::text), 10 + floor(random() * 20)::int) AS l_comment
                FROM tmp_orders AS o,
                     generate_series(1, 1 + ((o.o_orderkey::bigint * 7919) % 7)::int) AS n
                OFFSET 0
            ) AS g"""),
        ("orders", """
            INSERT INTO orders
            SELECT t.o_orderkey, t.o_custkey,
                   CASE WHEN a.all_f THEN 'F' WHEN a.all_o THEN 'O' ELSE 'P' END,
                   a.total, t.o_orderdate, t.o_orderpriority, t.o_clerk,
                   t.o_shippriority, t.o_comment
            FROM tmp_orders AS t
            JOIN (SELECT l_orderkey,
                         SUM(l_extendedprice * (1 + l_tax) * (1 - l_discount))::numeric(15,2) AS total,
                         bool_and(l_linestatus = 'F') AS all_f,
                         bool_and(l_linestatus = 'O') AS all_o
                  FROM lineitem GROUP BY l_orderkey) AS a ON a.l_orderkey = t.o_orderkey
            ORDER BY t.o_orderkey"""),
        ("čišćenje", "DROP TABLE tmp_orders"),
    ]
    return steps


# ---------------------------------------------------------------------------
# TPC-H-like upiti
# ---------------------------------------------------------------------------
# "sql"      - originalna formulacija (neki upiti su NAMERNO napisani onako
#              kako bi ih napisao neiskusan programer: Q14 koristi funkciju nad
#              kolonom, Q17 korelisani podupit)
# "prepisan" - semantički ekvivalentna, optimizovana formulacija (Faza 3)
# "mv"       - formulacija nad materijalizovanim pogledom (Faza 5)

QUERIES = {
    "Q1": {
        "naziv": "Izveštaj o cenama (pricing summary)",
        "sql": """
SELECT l_returnflag, l_linestatus,
       SUM(l_quantity) AS sum_qty,
       SUM(l_extendedprice) AS sum_base_price,
       SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
       SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
       AVG(l_quantity) AS avg_qty,
       AVG(l_extendedprice) AS avg_price,
       AVG(l_discount) AS avg_disc,
       COUNT(*) AS count_order
FROM lineitem
WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90 day'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus""",
        "mv": """
SELECT l_returnflag, l_linestatus,
       SUM(sum_qty) AS sum_qty,
       SUM(sum_base_price) AS sum_base_price,
       SUM(sum_disc_price) AS sum_disc_price,
       SUM(sum_charge) AS sum_charge,
       SUM(sum_qty) / SUM(cnt) AS avg_qty,
       SUM(sum_base_price) / SUM(cnt) AS avg_price,
       SUM(sum_disc) / SUM(cnt) AS avg_disc,
       SUM(cnt) AS count_order
FROM mv_lineitem_daily
WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90 day'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus""",
    },
    "Q3": {
        "naziv": "Prioritet isporuke (shipping priority)",
        "sql": """
SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue,
       o_orderdate, o_shippriority
FROM customer, orders, lineitem
WHERE c_mktsegment = 'BUILDING'
  AND c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate < DATE '1995-03-15'
  AND l_shipdate > DATE '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate
LIMIT 10""",
    },
    "Q5": {
        "naziv": "Promet lokalnih dobavljača (local supplier volume)",
        "sql": """
SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM customer, orders, lineitem, supplier, nation, region
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'ASIA'
  AND o_orderdate >= DATE '1994-01-01'
  AND o_orderdate < DATE '1994-01-01' + INTERVAL '1 year'
GROUP BY n_name
ORDER BY revenue DESC""",
    },
    "Q6": {
        "naziv": "Prognoza promene prihoda (forecasting revenue change)",
        "sql": """
SELECT SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= DATE '1994-01-01'
  AND l_shipdate < DATE '1994-01-01' + INTERVAL '1 year'
  AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
  AND l_quantity < 24""",
    },
    "Q10": {
        "naziv": "Vraćena roba (returned item reporting)",
        "sql": """
SELECT c_custkey, c_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue,
       c_acctbal, n_name, c_address, c_phone
FROM customer, orders, lineitem, nation
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate >= DATE '1993-10-01'
  AND o_orderdate < DATE '1993-10-01' + INTERVAL '3 month'
  AND l_returnflag = 'R'
  AND c_nationkey = n_nationkey
GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address
ORDER BY revenue DESC
LIMIT 20""",
    },
    "Q12": {
        "naziv": "Načini isporuke i prioritet (shipping modes)",
        "sql": """
SELECT l_shipmode,
       SUM(CASE WHEN o_orderpriority IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS high_line_count,
       SUM(CASE WHEN o_orderpriority NOT IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS low_line_count
FROM orders, lineitem
WHERE o_orderkey = l_orderkey
  AND l_shipmode IN ('MAIL', 'SHIP')
  AND l_commitdate < l_receiptdate
  AND l_shipdate < l_commitdate
  AND l_receiptdate >= DATE '1994-01-01'
  AND l_receiptdate < DATE '1994-01-01' + INTERVAL '1 year'
GROUP BY l_shipmode
ORDER BY l_shipmode""",
    },
    "Q14": {
        "naziv": "Efekat promocije (promotion effect)",
        # Ne-sargable uslov: funkcija nad kolonom sprečava upotrebu indeksa.
        "sql": """
SELECT 100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%'
                         THEN l_extendedprice * (1 - l_discount) ELSE 0 END)
       / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM lineitem, part
WHERE l_partkey = p_partkey
  AND date_trunc('month', l_shipdate) = DATE '1995-09-01'""",
        # Sargable: opseg nad samom kolonom - indeks na l_shipdate je upotrebljiv.
        "prepisan": """
SELECT 100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%'
                         THEN l_extendedprice * (1 - l_discount) ELSE 0 END)
       / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM lineitem, part
WHERE l_partkey = p_partkey
  AND l_shipdate >= DATE '1995-09-01'
  AND l_shipdate < DATE '1995-09-01' + INTERVAL '1 month'""",
    },
    "Q17": {
        "naziv": "Prihod od malih porudžbina (small-quantity-order revenue)",
        # Korelisani podupit se izvršava jednom za SVAKI red spoljnog upita.
        "sql": """
SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly
FROM lineitem, part
WHERE p_partkey = l_partkey
  AND p_brand = 'Brand#23'
  AND p_container = 'MED BOX'
  AND l_quantity < (SELECT 0.2 * AVG(l_quantity)
                    FROM lineitem
                    WHERE l_partkey = p_partkey)""",
        # Dekorelacija: prosek se računa JEDNOM po delu (GROUP BY), pa se spaja.
        "prepisan": """
WITH part_avg AS (
    SELECT l_partkey AS agg_partkey, 0.2 * AVG(l_quantity) AS avg_qty
    FROM lineitem
    JOIN part ON p_partkey = l_partkey
    WHERE p_brand = 'Brand#23'
      AND p_container = 'MED BOX'
    GROUP BY l_partkey
)
SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly
FROM lineitem
JOIN part_avg ON l_partkey = agg_partkey
WHERE l_quantity < avg_qty""",
    },
    "Q18": {
        "naziv": "Veliki kupci (large volume customer)",
        "sql": """
SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, SUM(l_quantity)
FROM customer, orders, lineitem
WHERE o_orderkey IN (SELECT l_orderkey
                     FROM lineitem
                     GROUP BY l_orderkey
                     HAVING SUM(l_quantity) > 300)
  AND c_custkey = o_custkey
  AND o_orderkey = l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate
LIMIT 100""",
    },
}


# ---------------------------------------------------------------------------
# Konfiguracije sesije
# ---------------------------------------------------------------------------
# Faze 0-3 koriste podrazumevane vrednosti PostgreSQL-a (eksplicitno
# postavljene, da rezultat ne zavisi od postgresql.conf na konkretnom serveru).
# Faza 4 uvodi podešavanja tipična za analitičko opterećenje.

DEFAULT_SETTINGS = {
    "work_mem": "4MB",
    "max_parallel_workers_per_gather": "2",
    "random_page_cost": "4",
    "effective_cache_size": "4GB",
    "jit": "on",
}

TUNED_SETTINGS = {
    "work_mem": "256MB",                    # sort/hash u memoriji umesto na disku
    "max_parallel_workers_per_gather": "4",  # više paralelnih radnika po upitu
    "random_page_cost": "1.1",              # SSD: nasumično čitanje ~ sekvencijalno
    "effective_cache_size": "8GB",          # procena keša OS-a za optimizator
    "jit": "off",                           # JIT prevođenje se ne isplati za upite < 1 s
}


# ---------------------------------------------------------------------------
# Merenje
# ---------------------------------------------------------------------------

def normalize_result(rows):
    """Heš rezultata upita (za proveru da optimizacija ne menja rezultat)."""
    norm = []
    for r in rows:
        vals = []
        for v in r:
            # int i Decimal se normalizuju isto (npr. SUM(bigint) vraća numeric, COUNT(*) bigint)
            if isinstance(v, (Decimal, int)) and not isinstance(v, bool):
                vals.append(str(Decimal(v).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)))
            elif isinstance(v, float):
                vals.append(f"{v:.2f}")
            else:
                vals.append(str(v).strip())
        norm.append(tuple(vals))
    norm.sort()
    return hashlib.md5(repr(norm).encode()).hexdigest()


def apply_settings(conn, settings, timeout_s):
    with conn.cursor() as cur:
        for k, v in settings.items():
            cur.execute(f"SET {k} = %s", (v,))
        cur.execute("SET statement_timeout = %s", (int(timeout_s * 1000),))


def run_query(conn, sql, runs):
    """1 zagrevanje + `runs` merenja. Vraća rečnik sa rezultatima."""
    times = []
    rows = None
    with conn.cursor() as cur:
        for i in range(runs + 1):
            t0 = time.perf_counter()
            try:
                cur.execute(sql)
                rows = cur.fetchall()
            except psycopg2.errors.QueryCanceled:
                return {"timeout": True, "times": [], "rows": None}
            dt = (time.perf_counter() - t0) * 1000
            if i > 0:  # prvo izvršavanje je zagrevanje (keš)
                times.append(dt)
    return {"timeout": False, "times": times, "rows": rows}


BUF_RE = re.compile(r"Buffers: shared(?: hit=(\d+))?(?: read=(\d+))?(?: dirtied=\d+)?(?: written=\d+)?"
                    r"(?:, temp(?: read=(\d+))?(?: written=(\d+))?)?")


def explain(conn, sql, analyze=True):
    """Vraća tekst plana i broj pročitanih blokova (iz korenskog čvora)."""
    opts = "ANALYZE, BUFFERS" if analyze else "COSTS"
    with conn.cursor() as cur:
        try:
            cur.execute(f"EXPLAIN ({opts}) {sql}")
        except psycopg2.errors.QueryCanceled:
            cur.execute(f"EXPLAIN {sql}")
            analyze = False
        text = "\n".join(r[0] for r in cur.fetchall())
    shared = temp = None
    if analyze:
        m = BUF_RE.search(text)
        if m:
            shared = int(m.group(1) or 0) + int(m.group(2) or 0)
            temp = int(m.group(3) or 0) + int(m.group(4) or 0)
    return text, shared, temp, analyze


# ---------------------------------------------------------------------------
# Akcije pojedinih faza (vraćaju listu (akcija, trajanje_s) - "cena optimizacije")
# ---------------------------------------------------------------------------

def cardinality_demo(conn):
    """Poredi procenu broja redova optimizatora sa stvarnim brojem redova."""
    predicates = [
        ("lineitem", "l_returnflag = 'R'"),
        ("lineitem", "l_shipdate BETWEEN DATE '1995-09-01' AND DATE '1995-09-30'"),
        ("orders", "o_orderpriority = '1-URGENT' AND o_orderdate >= DATE '1997-01-01'"),
        ("customer", "c_mktsegment = 'BUILDING'"),
    ]
    out = []
    with conn.cursor() as cur:
        for table, pred in predicates:
            cur.execute(f"EXPLAIN (FORMAT JSON) SELECT * FROM {table} WHERE {pred}")
            plan = cur.fetchone()[0]
            if isinstance(plan, str):
                plan = json.loads(plan)
            est = int(plan[0]["Plan"]["Plan Rows"])
            cur.execute(f"SELECT count(*) FROM {table} WHERE {pred}")
            act = cur.fetchone()[0]
            factor = max(est, 1) / max(act, 1)
            factor = factor if factor >= 1 else 1 / factor
            out.append((f"{table}: {pred}", est, act, factor))
    return out


def phase_analyze(conn):
    costs = []
    for t in TABLES:
        costs.append((f"ANALYZE {t}", exec_timed(conn, f"ANALYZE {t}")))
    return costs


INDEX_DDL = [
    # Primarni ključevi (jedinstveni B-tree indeksi)
    "ALTER TABLE region   ADD PRIMARY KEY (r_regionkey)",
    "ALTER TABLE nation   ADD PRIMARY KEY (n_nationkey)",
    "ALTER TABLE supplier ADD PRIMARY KEY (s_suppkey)",
    "ALTER TABLE part     ADD PRIMARY KEY (p_partkey)",
    "ALTER TABLE partsupp ADD PRIMARY KEY (ps_partkey, ps_suppkey)",
    "ALTER TABLE customer ADD PRIMARY KEY (c_custkey)",
    "ALTER TABLE orders   ADD PRIMARY KEY (o_orderkey)",
    "ALTER TABLE lineitem ADD PRIMARY KEY (l_orderkey, l_linenumber)",
    # Strani ključevi - integritet + bolje procene kardinalnosti spajanja
    "ALTER TABLE nation   ADD FOREIGN KEY (n_regionkey) REFERENCES region",
    "ALTER TABLE supplier ADD FOREIGN KEY (s_nationkey) REFERENCES nation",
    "ALTER TABLE customer ADD FOREIGN KEY (c_nationkey) REFERENCES nation",
    "ALTER TABLE partsupp ADD FOREIGN KEY (ps_partkey)  REFERENCES part",
    "ALTER TABLE partsupp ADD FOREIGN KEY (ps_suppkey)  REFERENCES supplier",
    "ALTER TABLE orders   ADD FOREIGN KEY (o_custkey)   REFERENCES customer",
    "ALTER TABLE lineitem ADD FOREIGN KEY (l_orderkey)  REFERENCES orders",
    "ALTER TABLE lineitem ADD FOREIGN KEY (l_partkey)   REFERENCES part",
    "ALTER TABLE lineitem ADD FOREIGN KEY (l_suppkey)   REFERENCES supplier",
    # Indeksi nad kolonama stranih ključeva (PostgreSQL ih NE pravi automatski)
    "CREATE INDEX idx_orders_custkey   ON orders   (o_custkey)",
    "CREATE INDEX idx_lineitem_partkey ON lineitem (l_partkey)",
    # Indeks za filtriranje po datumu
    "CREATE INDEX idx_orders_orderdate ON orders   (o_orderdate)",
    # Pokrivajući indeks (INCLUDE) - omogućava Index Only Scan za Q6 i Q14
    "CREATE INDEX idx_lineitem_shipdate ON lineitem (l_shipdate) "
    "INCLUDE (l_partkey, l_extendedprice, l_discount, l_quantity)",
    # Kompozitni indeks za uslov nad dve kolone (Q17)
    "CREATE INDEX idx_part_brand_container ON part (p_brand, p_container)",
]


def phase_indexes(conn):
    costs = []
    for ddl in INDEX_DDL:
        label = " ".join(ddl.split())
        costs.append((label, exec_timed(conn, ddl)))
    # VACUUM postavlja visibility map (preduslov za Index Only Scan),
    # ANALYZE osvežava statistiku (i za nove indekse).
    for t in TABLES:
        costs.append((f"VACUUM (ANALYZE) {t}", exec_timed(conn, f"VACUUM (ANALYZE) {t}")))
    return costs


MV_DDL = """
CREATE MATERIALIZED VIEW mv_lineitem_daily AS
SELECT l_returnflag, l_linestatus, l_shipdate,
       SUM(l_quantity) AS sum_qty,
       SUM(l_extendedprice) AS sum_base_price,
       SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
       SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
       SUM(l_discount) AS sum_disc,
       COUNT(*) AS cnt
FROM lineitem
GROUP BY l_returnflag, l_linestatus, l_shipdate
"""


def phase_mv(conn):
    costs = [("CREATE MATERIALIZED VIEW mv_lineitem_daily", exec_timed(conn, MV_DDL)),
             ("ANALYZE mv_lineitem_daily", exec_timed(conn, "ANALYZE mv_lineitem_daily")),
             ("REFRESH MATERIALIZED VIEW mv_lineitem_daily (cena osvežavanja)",
              exec_timed(conn, "REFRESH MATERIALIZED VIEW mv_lineitem_daily"))]
    return costs


PHASES = [
    {"id": 0, "kratko": "F0 Baseline", "naziv": "Baseline",
     "opis": "Tabele bez PK/indeksa, bez statistike (autovacuum isključen), podrazumevana konfiguracija.",
     "setup": None, "rewrite": False, "mv": False, "tuned": False},
    {"id": 1, "kratko": "F1 ANALYZE", "naziv": "ANALYZE - statistika",
     "opis": "ANALYZE prikuplja statistiku (histogrami, MCV, n_distinct) - optimizator bolje procenjuje kardinalnost.",
     "setup": phase_analyze, "rewrite": False, "mv": False, "tuned": False},
    {"id": 2, "kratko": "F2 Indeksi", "naziv": "Indeksi",
     "opis": "PK, FK, indeksi nad FK kolonama, indeks za filter po datumu, pokrivajući i kompozitni indeks + VACUUM.",
     "setup": phase_indexes, "rewrite": False, "mv": False, "tuned": False},
    {"id": 3, "kratko": "F3 Prepisani", "naziv": "Prepisivanje upita",
     "opis": "Q14: funkcija nad kolonom -> opseg (sargable); Q17: korelisani podupit -> JOIN sa preagregacijom.",
     "setup": None, "rewrite": True, "mv": False, "tuned": False},
    {"id": 4, "kratko": "F4 Konfig.", "naziv": "Podešavanje konfiguracije",
     "opis": "work_mem=256MB, max_parallel_workers_per_gather=4, random_page_cost=1.1, jit=off.",
     "setup": None, "rewrite": True, "mv": False, "tuned": True},
    {"id": 5, "kratko": "F5 Mat. pogled", "naziv": "Materijalizovani pogled",
     "opis": "Q1 čita unapred agregirane podatke po danu iz materijalizovanog pogleda.",
     "setup": phase_mv, "rewrite": True, "mv": True, "tuned": True},
]


def pick_sql(q, phase):
    if phase["mv"] and q.get("mv"):
        return q["mv"], "mat. pogled"
    if phase["rewrite"] and q.get("prepisan"):
        return q["prepisan"], "prepisan"
    return q["sql"], "original"


# ---------------------------------------------------------------------------
# Throughput test
# ---------------------------------------------------------------------------

def throughput_test(args, phase, n_streams):
    """Pokreće n paralelnih tokova; svaki izvršava sve upite u permutovanom redosledu."""
    settings = TUNED_SETTINGS if phase["tuned"] else DEFAULT_SETTINGS
    errors = []
    barrier = threading.Barrier(n_streams + 1)

    def worker(stream_id):
        try:
            conn = connect(args)
            apply_settings(conn, settings, args.timeout)
            order = list(QUERIES.keys())
            random.Random(1000 + stream_id).shuffle(order)
            barrier.wait()
            with conn.cursor() as cur:
                for qid in order:
                    sql, _ = pick_sql(QUERIES[qid], phase)
                    cur.execute(sql)
                    cur.fetchall()
            conn.close()
        except Exception as e:  # noqa: BLE001 - greška se prijavljuje korisniku
            errors.append(str(e))
            try:
                barrier.abort()
            except threading.BrokenBarrierError:
                pass

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_streams)]
    for t in threads:
        t.start()
    try:
        barrier.wait()
    except threading.BrokenBarrierError:
        pass
    t0 = time.perf_counter()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - t0
    if errors:
        raise RuntimeError(errors[0])
    return elapsed


# ---------------------------------------------------------------------------
# Grafici
# ---------------------------------------------------------------------------

PALETTE = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
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


def make_charts(out_dir, results, phases, timeout_s, throughput):
    if not HAVE_MPL:
        info("matplotlib nije instaliran - grafici se preskaču (pip install matplotlib).")
        return []
    files = []
    qids = list(QUERIES.keys())

    # 1) Vreme izvršavanja po upitu i fazi (log skala)
    fig, ax = plt.subplots(figsize=(12, 5.5), facecolor=SURFACE)
    n = len(phases)
    width = min(0.8 / n, 0.14)
    for pi, ph in enumerate(phases):
        xs, ys, hatches = [], [], []
        for qi, qid in enumerate(qids):
            r = results[(ph["id"], qid)]
            xs.append(qi + (pi - (n - 1) / 2) * width)
            ys.append(r["median"])
            hatches.append(r["timeout"])
        bars = ax.bar(xs, ys, width * 0.9, color=PALETTE[pi % len(PALETTE)], label=ph["kratko"],
                      edgecolor=SURFACE, linewidth=0.5)
        for b, h in zip(bars, hatches):
            if h:
                b.set_hatch("///")
                b.set_edgecolor(INK2)
    ax.set_yscale("log")
    ax.set_xticks(range(len(qids)))
    ax.set_xticklabels(qids)
    style_axes(ax, "Vreme izvršavanja upita po fazama optimizacije (medijana, log skala)", "vreme [ms]")
    ax.axhline(timeout_s * 1000, color=MUTED, linewidth=1)
    ax.text(len(qids) - 0.5, timeout_s * 1000 * 1.08, f"timeout {timeout_s:g} s (šrafirano)",
            color=INK2, fontsize=8, ha="right")
    ax.legend(ncol=n, loc="upper center", bbox_to_anchor=(0.5, -0.08), frameon=False,
              labelcolor=INK2, fontsize=9)
    fig.tight_layout()
    f = out_dir / "grafik_upiti_po_fazama.png"
    fig.savefig(f, dpi=150, facecolor=SURFACE)
    plt.close(fig)
    files.append(f)

    # 2) Geometrijska sredina po fazi
    fig, ax = plt.subplots(figsize=(9, 4.8), facecolor=SURFACE)
    labels = [ph["kratko"] for ph in phases]
    gms = [statistics.geometric_mean([results[(ph["id"], q)]["median"] for q in qids]) for ph in phases]
    bars = ax.bar(range(len(phases)), gms, 0.5, color=PALETTE[0])
    for i, (b, g) in enumerate(zip(bars, gms)):
        ax.text(b.get_x() + b.get_width() / 2, g, f"{g:.0f} ms\n×{gms[0] / g:.1f}",
                ha="center", va="bottom", color=INK2, fontsize=9)
    ax.set_xticks(range(len(phases)))
    ax.set_xticklabels(labels, fontsize=9)
    ax.set_ylim(0, max(gms) * 1.25)
    style_axes(ax, "Geometrijska sredina vremena upita po fazi (×ubrzanje u odnosu na baseline)", "vreme [ms]")
    fig.tight_layout()
    f = out_dir / "grafik_geomean_po_fazama.png"
    fig.savefig(f, dpi=150, facecolor=SURFACE)
    plt.close(fig)
    files.append(f)

    # 3) Throughput
    if throughput:
        fig, ax = plt.subplots(figsize=(7, 4.5), facecolor=SURFACE)
        ns = [t["streams"] for t in throughput]
        qph = [t["qph"] for t in throughput]
        bars = ax.bar(range(len(ns)), qph, 0.5, color=PALETTE[0])
        for b, v in zip(bars, qph):
            ax.text(b.get_x() + b.get_width() / 2, v, f"{v:,.0f}", ha="center", va="bottom",
                    color=INK2, fontsize=9)
        ax.set_xticks(range(len(ns)))
        ax.set_xticklabels([f"{n} tok(a)" for n in ns])
        ax.set_ylim(0, max(qph) * 1.2)
        style_axes(ax, "Propusna moć: upita na sat (Throughput test, finalna faza)", "upita / sat")
        fig.tight_layout()
        f = out_dir / "grafik_throughput.png"
        fig.savefig(f, dpi=150, facecolor=SURFACE)
        plt.close(fig)
        files.append(f)
    return files


# ---------------------------------------------------------------------------
# Glavni tok
# ---------------------------------------------------------------------------

def parse_args():
    here = Path(__file__).resolve().parent
    p = argparse.ArgumentParser(description="TPC-like benchmark i optimizacija upita (PostgreSQL)")
    p.add_argument("--host", default=os.environ.get("PGHOST", "localhost"))
    p.add_argument("--port", type=int, default=int(os.environ.get("PGPORT", 5432)))
    p.add_argument("--user", default=os.environ.get("PGUSER", "postgres"))
    p.add_argument("--password", default=os.environ.get("PGPASSWORD"))
    p.add_argument("--db", default="tpc_demo", help="ime baze koja se kreira (default: tpc_demo)")
    p.add_argument("--scale", type=float, default=0.3,
                   help="faktor skaliranja SF (0.3 ~ 1,8M stavki lineitem; 1.0 ~ 6M; 0.1 za brzi test)")
    p.add_argument("--runs", type=int, default=3, help="broj merenja po upitu (posle 1 zagrevanja)")
    p.add_argument("--timeout", type=float, default=60, help="statement_timeout u sekundama")
    p.add_argument("--streams", default="1,2,4",
                   help="broj paralelnih tokova za throughput test, npr. '1,2,4' ('0' = preskoči)")
    p.add_argument("--out", default=str(here / "rezultati"), help="direktorijum za rezultate")
    p.add_argument("--keep", action="store_true", help="ne brisati bazu na kraju")
    return p.parse_args()


def main():
    args = parse_args()
    args.scale = max(0.01, args.scale)
    out_dir = Path(args.out)
    plans_dir = out_dir / "planovi"
    plans_dir.mkdir(parents=True, exist_ok=True)
    started = datetime.now()

    # ------------------------------------------------------------------ 1
    korak(1, "Priprema okruženja")
    try:
        admin = connect(args, dbname="postgres")
    except psycopg2.OperationalError as e:
        sys.exit(f"Ne mogu da se povežem na PostgreSQL ({args.host}:{args.port}): {e}")
    pg_version = scalar(admin, "SHOW server_version")
    info(f"PostgreSQL verzija : {pg_version}")
    info(f"Server             : {args.host}:{args.port}, korisnik {args.user}")
    info(f"Faktor skaliranja  : SF = {args.scale}")
    info(f"Merenja po upitu   : 1 zagrevanje + {args.runs} merenja (medijana), timeout {args.timeout:g} s")
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {args.db} WITH (FORCE)")
        cur.execute(f"CREATE DATABASE {args.db}")
    admin.close()
    info(f"Kreirana baza '{args.db}'.")
    conn = connect(args)
    server_settings = {}
    with conn.cursor() as cur:
        for s in ("shared_buffers", "max_worker_processes", "max_parallel_workers"):
            cur.execute(f"SHOW {s}")
            server_settings[s] = cur.fetchone()[0]
    info("Serverska podešavanja: " + ", ".join(f"{k}={v}" for k, v in server_settings.items()))
    info(f"Broj CPU jezgara (klijent): {os.cpu_count()}")

    # ------------------------------------------------------------------ 2
    korak(2, "Kreiranje TPC-H-like šeme")
    exec_timed(conn, SCHEMA_DDL)
    info("Kreirane tabele: " + ", ".join(TABLES))
    info("Bez primarnih ključeva i indeksa; autovacuum_enabled = false (nema automatske statistike).")

    # ------------------------------------------------------------------ 3
    korak(3, f"Generisanje podataka (SF = {args.scale})")
    load_total = 0.0
    for i, (name, sql) in enumerate(data_generation_steps(args.scale)):
        with conn.cursor() as cur:
            cur.execute("SELECT setseed(%s)", ((i + 1) / 100,))
        dt = exec_timed(conn, sql)
        load_total += dt
        info(f"{name:<22} {dt:7.2f} s")
    table_stats = []
    with conn.cursor() as cur:
        for t in TABLES:
            cur.execute(f"SELECT count(*), pg_total_relation_size('{t}') FROM {t}")
            cnt, size = cur.fetchone()
            table_stats.append((t, cnt, size))
    print()
    print_table(["tabela", "broj redova", "veličina"],
                [(t, f"{c:,}", fmt_bytes(s)) for t, c, s in table_stats])
    info(f"Ukupno vreme učitavanja: {load_total:.1f} s")

    # ------------------------------------------------------------ 4..9
    results = {}
    reference = {}
    costs_all = []
    card_demo = {}
    plan_texts = {}
    step_no = 4
    for ph in PHASES:
        korak(step_no, f"Faza {ph['id']} - {ph['naziv']}")
        step_no += 1
        info(ph["opis"])
        if ph["setup"]:
            info("Primena optimizacije...")
            costs = ph["setup"](conn)
            for label, dt in costs:
                costs_all.append((ph["id"], label, dt))
            info(f"  trajanje pripreme: {sum(c[1] for c in costs):.2f} s ({len(costs)} naredbi)")

        if ph["id"] in (0, 1):
            demo = cardinality_demo(conn)
            card_demo[ph["id"]] = demo
            info("Procena kardinalnosti (optimizator vs. stvarnost):")
            for pred, est, act, factor in demo:
                info(f"  {pred[:62]:<62} procena {est:>9,}  stvarno {act:>9,}  greška ×{factor:.1f}")

        settings = TUNED_SETTINGS if ph["tuned"] else DEFAULT_SETTINGS
        bconn = connect(args)
        apply_settings(bconn, settings, args.timeout)
        print()
        for qid, q in QUERIES.items():
            sql, variant = pick_sql(q, ph)
            r = run_query(bconn, sql, args.runs)
            if r["timeout"]:
                median = mn = mx = args.timeout * 1000
            else:
                median = statistics.median(r["times"])
                mn, mx = min(r["times"]), max(r["times"])
            plan, shared, temp, analyzed = explain(bconn, sql, analyze=not r["timeout"])
            plan_texts[(ph["id"], qid)] = plan
            with open(plans_dir / f"faza{ph['id']}_{qid}.txt", "w", encoding="utf-8") as f:
                f.write(f"-- Faza {ph['id']} ({ph['naziv']}), upit {qid} [{variant}]\n")
                f.write(f"-- Podešavanja: {settings}\n")
                if not analyzed:
                    f.write("-- EXPLAIN bez ANALYZE (upit je prekoračio timeout)\n")
                f.write(sql.strip() + "\n\n" + plan + "\n")
            # provera ispravnosti rezultata
            ok = None
            if not r["timeout"]:
                h = normalize_result(r["rows"])
                if qid not in reference:
                    reference[qid] = h
                    ok = True
                else:
                    ok = reference[qid] == h
            results[(ph["id"], qid)] = {
                "median": median, "min": mn, "max": mx, "timeout": r["timeout"],
                "shared": shared, "temp": temp, "variant": variant, "ok": ok,
            }
            status = "TIMEOUT" if r["timeout"] else ("OK" if ok else "RAZLIČIT REZULTAT!")
            buf = f"blokova {shared:>8,}" if shared is not None else "blokova        -"
            tmp = f" temp {temp:,}" if temp else ""
            info(f"{qid:<4} {q['naziv'][:44]:<44} {fmt_ms(median, r['timeout']):>12}  "
                 f"{buf}{tmp}  [{variant}] {status}")
        bconn.close()
        gm = statistics.geometric_mean([results[(ph["id"], q)]["median"] for q in QUERIES])
        info(f"Geometrijska sredina faze: {gm:.1f} ms")

    # ------------------------------------------------------------------ 10
    final = PHASES[-1]
    throughput = []
    streams = [int(s) for s in args.streams.split(",") if s.strip() and int(s) > 0]
    korak(step_no, "Throughput test (paralelni tokovi upita, finalna faza)")
    step_no += 1
    if not streams:
        info("Preskočeno (--streams 0).")
    for n in streams:
        try:
            elapsed = throughput_test(args, final, n)
        except Exception as e:  # noqa: BLE001
            info(f"{n} tok(a): greška - {e}")
            continue
        total_q = n * len(QUERIES)
        qph = total_q * 3600 / elapsed
        throughput.append({"streams": n, "elapsed": elapsed, "queries": total_q, "qph": qph,
                           "qph_sf": qph * args.scale})
        info(f"{n} tok(a): {total_q} upita za {elapsed:.2f} s -> {qph:,.0f} upita/sat "
             f"(Throughput-like = {qph * args.scale:,.1f})")

    # ------------------------------------------------------------------ 11
    korak(step_no, "Zbirni rezultati")
    qids = list(QUERIES.keys())
    headers = ["upit"] + [ph["kratko"] for ph in PHASES] + ["ubrzanje"]
    rows = []
    for qid in qids:
        base = results[(0, qid)]
        last = results[(PHASES[-1]["id"], qid)]
        row = [qid] + [fmt_ms(results[(ph["id"], qid)]["median"], results[(ph["id"], qid)]["timeout"])
                       for ph in PHASES]
        sp = base["median"] / last["median"]
        row.append(f"{'>=' if base['timeout'] else ''}×{sp:,.1f}")
        rows.append(row)
    gms = {ph["id"]: statistics.geometric_mean([results[(ph["id"], q)]["median"] for q in qids])
           for ph in PHASES}
    rows.append(["geomean"] + [fmt_ms(gms[ph["id"]]) for ph in PHASES] +
                [f"×{gms[0] / gms[PHASES[-1]['id']]:,.1f}"])
    power = {pid: 3600 * args.scale / (g / 1000) for pid, g in gms.items()}
    rows.append(["Power-like"] + [f"{power[ph['id']]:,.0f}" for ph in PHASES] + [""])
    print_table(headers, rows)
    info("")
    info("Power-like = 3600 · SF / geomean[s] (inspirisano TPC-H Power@Size metrikom).")

    # Regresije: upit sporiji za > 20% (i > 5 ms) u odnosu na prethodnu fazu.
    # Optimizacija koja pomaže većini upita može da pogorša pojedinačne - zato se meri.
    regressions = []
    for qid in qids:
        for prev, cur in zip(PHASES, PHASES[1:]):
            a, b = results[(prev["id"], qid)], results[(cur["id"], qid)]
            if not a["timeout"] and b["median"] > a["median"] * 1.2 and b["median"] - a["median"] > 5:
                regressions.append((qid, prev["kratko"], cur["kratko"], a["median"], b["median"],
                                    f"planovi/faza{prev['id']}_{qid}.txt vs planovi/faza{cur['id']}_{qid}.txt"))
    if regressions:
        print()
        info("Regresije (upit sporiji za >20% nego u prethodnoj fazi) - uporedite planove:")
        for qid, pa, pb, ta, tb, files in regressions:
            info(f"  {qid}: {pa} {fmt_ms(ta)} -> {pb} {fmt_ms(tb)}   ({files})")

    # Pre/posle planovi za dva ključna upita
    for qid, before, after in (("Q14", 1, 3), ("Q17", 1, 3)):
        print()
        info(f"--- Plan {qid}: Faza {before} (pre) ---")
        for line in plan_texts[(before, qid)].splitlines()[:14]:
            info("  " + line)
        info(f"--- Plan {qid}: Faza {after} (posle) ---")
        for line in plan_texts[(after, qid)].splitlines()[:14]:
            info("  " + line)

    # Cena optimizacije
    with conn.cursor() as cur:
        cur.execute("""SELECT indexrelname, pg_relation_size(indexrelid)
                       FROM pg_stat_user_indexes ORDER BY 2 DESC""")
        idx_sizes = cur.fetchall()
        cur.execute("SELECT pg_total_relation_size('mv_lineitem_daily'), (SELECT count(*) FROM mv_lineitem_daily)")
        mv_size, mv_rows = cur.fetchone()
    data_size = sum(s for _, _, s in table_stats)
    idx_total = sum(s for _, s in idx_sizes)
    print()
    info("Cena optimizacije (prostor):")
    info(f"  podaci (pre indeksa): {fmt_bytes(data_size)}; indeksi: {fmt_bytes(idx_total)} "
         f"({100 * idx_total / data_size:.0f}% podataka); mat. pogled: {fmt_bytes(mv_size)} ({mv_rows:,} redova)")

    # ---------------------------------------------------------- izlazni fajlovi
    csv_path = out_dir / "rezultati.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["faza_id", "faza", "upit", "varijanta", "medijana_ms", "min_ms", "max_ms",
                    "timeout", "blokovi_shared", "blokovi_temp", "rezultat_ok"])
        for ph in PHASES:
            for qid in qids:
                r = results[(ph["id"], qid)]
                w.writerow([ph["id"], ph["naziv"], qid, r["variant"], f"{r['median']:.2f}",
                            f"{r['min']:.2f}", f"{r['max']:.2f}", r["timeout"], r["shared"],
                            r["temp"], r["ok"]])
    charts = make_charts(out_dir, results, PHASES, args.timeout, throughput)

    md = []
    md.append("# Optimizacija upita primenom TPC-like benchmarking pristupa - rezultati\n")
    md.append(f"Datum: {started:%d.%m.%Y %H:%M} · PostgreSQL {pg_version} · SF = {args.scale} · "
              f"{args.runs} merenja po upitu (medijana, posle 1 zagrevanja) · timeout {args.timeout:g} s\n")
    md.append("## 1. Podaci\n")
    md.append(md_table(["tabela", "broj redova", "veličina"],
                       [(t, f"{c:,}", fmt_bytes(s)) for t, c, s in table_stats]))
    md.append(f"\nVreme generisanja podataka: {load_total:.1f} s\n")
    md.append("## 2. Faze optimizacije\n")
    for ph in PHASES:
        md.append(f"- **Faza {ph['id']} - {ph['naziv']}**: {ph['opis']}")
    md.append("\n## 3. Procena kardinalnosti pre i posle ANALYZE\n")
    card_rows = []
    for i, (pred, est0, act, f0) in enumerate(card_demo.get(0, [])):
        est1, f1 = card_demo[1][i][1], card_demo[1][i][3]
        card_rows.append((f"`{pred}`", f"{act:,}", f"{est0:,} (×{f0:.1f})", f"{est1:,} (×{f1:.1f})"))
    md.append(md_table(["predikat", "stvarno", "procena bez statistike", "procena posle ANALYZE"], card_rows))
    md.append("\n## 4. Vreme izvršavanja upita (medijana)\n")
    md.append(md_table(headers, rows))
    md.append("\n`>=` označava upit koji je prekoračio timeout (stvarno vreme je veće). "
              "Power-like = 3600 · SF / geomean[s].\n")
    if regressions:
        md.append("### Regresije\n")
        md.append("Upiti koji su u nekoj fazi postali sporiji za više od 20% u odnosu na prethodnu fazu "
                  "(optimizacija koja pomaže većini upita može da pogorša pojedinačne - zato se svaka "
                  "promena proverava benchmarkom):\n")
        md.append(md_table(["upit", "pre", "posle", "planovi"],
                           [(q, f"{pa}: {fmt_ms(ta)}", f"{pb}: {fmt_ms(tb)}", f"`{fl}`")
                            for q, pa, pb, ta, tb, fl in regressions]))
        md.append("")
    md.append("## 5. Broj pročitanih blokova (shared hit + read, korenski čvor plana)\n")
    buf_rows = []
    for qid in qids:
        buf_rows.append([qid] + [f"{results[(ph['id'], qid)]['shared']:,}"
                                 if results[(ph['id'], qid)]['shared'] is not None else "-"
                                 for ph in PHASES])
    md.append(md_table(["upit"] + [ph["kratko"] for ph in PHASES], buf_rows))
    md.append("\n## 6. Cena optimizacije\n")
    cost_rows = [(f"F{pid}", f"`{label[:90]}`", f"{dt:.2f} s") for pid, label, dt in costs_all]
    md.append(md_table(["faza", "naredba", "trajanje"], cost_rows))
    md.append(f"\nVeličina podataka: {fmt_bytes(data_size)}, indeksi ukupno: {fmt_bytes(idx_total)}, "
              f"materijalizovani pogled: {fmt_bytes(mv_size)} ({mv_rows:,} redova).\n")
    md.append(md_table(["indeks", "veličina"], [(n, fmt_bytes(s)) for n, s in idx_sizes]))
    if throughput:
        md.append("\n## 7. Throughput test (finalna faza)\n")
        md.append(md_table(["tokova", "upita", "trajanje", "upita/sat", "Throughput-like (×SF)"],
                           [(t["streams"], t["queries"], f"{t['elapsed']:.2f} s", f"{t['qph']:,.0f}",
                             f"{t['qph_sf']:,.1f}") for t in throughput]))
        best = max(throughput, key=lambda t: t["qph_sf"])
        composite = math.sqrt(power[PHASES[-1]["id"]] * best["qph_sf"])
        md.append(f"\nKompozitna metrika (po uzoru na QphH@Size = sqrt(Power · Throughput)): "
                  f"**{composite:,.0f}**\n")
    md.append("\n## 8. Provera ispravnosti\n")
    bad = [(ph["id"], q) for ph in PHASES for q in qids if results[(ph["id"], q)]["ok"] is False]
    md.append("Svi optimizovani upiti vraćaju isti rezultat kao originalni." if not bad else
              f"UPOZORENJE: različiti rezultati za {bad}")
    md.append("\n## 9. Fajlovi\n")
    md.append("- `rezultati.csv` - sirovi rezultati merenja")
    md.append("- `planovi/fazaN_QX.txt` - EXPLAIN (ANALYZE, BUFFERS) planovi za svaki upit i fazu")
    for c in charts:
        md.append(f"- `{c.name}`\n\n![{c.stem}]({c.name})")
    report = out_dir / "izvestaj.md"
    report.write_text("\n".join(md) + "\n", encoding="utf-8")

    print()
    info(f"Rezultati sačuvani u: {out_dir}")
    info(f"  - {report.name}, {csv_path.name}, planovi/ ({len(list(plans_dir.iterdir()))} fajlova)")
    for c in charts:
        info(f"  - {c.name}")

    conn.close()
    if not args.keep:
        admin = connect(args, dbname="postgres")
        with admin.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {args.db} WITH (FORCE)")
        admin.close()
        info(f"Baza '{args.db}' obrisana (za zadržavanje koristite --keep).")
    else:
        info(f"Baza '{args.db}' je zadržana za dalju analizu (psql -d {args.db}).")
    info(f"Ukupno trajanje: {(datetime.now() - started).total_seconds():.0f} s")


if __name__ == "__main__":
    main()
