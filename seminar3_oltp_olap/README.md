# Seminarski 3 – OLTP i OLAP sistemi

Skripta `oltp_olap_demo.py` na jednom PostgreSQL serveru gradi **transakcioni
sistem** (online prodavnica, šema `oltp` u 3NF) i **skladište podataka** (šema
`dw`, zvezdasta šema), puni skladište ETL procesom i meri kako se ta dva sistema
ponašaju pod svojim tipičnim opterećenjem – i šta se desi kada se pomešaju.

## Pokretanje

```bash
pip install psycopg2-binary matplotlib
python oltp_olap_demo.py --host localhost --user postgres --password postgres
```

| parametar | podrazumevano | značenje |
|---|---|---|
| `--scale` | `1.0` | veličina podataka (1.0 = 20.000 kupaca, 2.000 proizvoda, 200.000 narudžbina, ~500.000 stavki) |
| `--threads` | `4` | broj konkurentnih OLTP klijenata |
| `--olap-threads` | `2` | broj niti sa analitičkim upitima u testu interferencije |
| `--duration` | `15` | trajanje svakog OLTP testa u sekundama |
| `--runs` | `3` | broj merenja po OLAP upitu posle zagrevanja (medijana) |
| `--db` | `oltp_olap_demo` | baza koja se kreira (postojeća baza istog imena se briše!) |
| `--keep` | – | zadrži bazu posle izvršavanja |
| `--out` | `./rezultati` | direktorijum za izveštaj, CSV i grafike |

Trajanje: ~1,5 min sa podrazumevanim parametrima.

## Koraci skripte i veza sa teorijom

| korak | šta radi | teorijsko poglavlje |
|---|---|---|
| 1 | kreira bazu | – |
| 2 | OLTP šema u **3NF**: `region → city → customer`, `category → product`, `orders → order_item, payment`; PK/FK/UNIQUE/CHECK; istorija narudžbina za ~3,5 godine (trend rasta, sezonalnost u novembru/decembru) | normalizacija, integritet podataka, OLTP model |
| 3 | **OLTP opterećenje**: N klijenata istovremeno izvršava kratke transakcije (50% nova narudžbina sa `SELECT … FOR UPDATE`, 30% čitanje po PK, 20% UPDATE statusa); meri TPS, latenciju p50/p95/p99 i broj ROLLBACK-ova; zatim **proverava ACID** invarijante | karakteristike OLTP-a, transakcije, ACID, konkurentnost i zaključavanje, deadlock |
| 4 | `EXPLAIN (ANALYZE, BUFFERS)` za OLTP upite i za analitički upit nad istom šemom: pročitani redovi, blokovi, tip pristupa | obrasci pristupa podacima, indeksi vs. skeniranje |
| 5 | **ETL** u zvezdastu šemu: `dim_date`, `dim_customer`, `dim_product` (denormalizovane, surogat ključevi) + `fact_sales`; BRIN indeks; materijalizovani pogled | skladište podataka, star schema, dimenzije i činjenice, ETL |
| 6 | **OLAP upiti**: ROLLUP (drill-down), CUBE, slice, dice, top-N (`RANK`), međugodišnji rast (`LAG`), kumulativ; isto pitanje nad 3NF vs star vs mat. pogled (uz proveru da su rezultati identični) | OLAP operacije, višedimenzionalni model, pre-agregacija |
| 7 | **Interferencija**: OLTP sam vs OLTP + analitički upiti nad istim tabelama | zašto se OLTP i OLAP razdvajaju |
| 8 | **Inkrementalni ETL** (watermark po `order_id`): DW pre ETL-a ne vidi nove narudžbine, posle je sinhronizovan | svežina podataka, periodično učitavanje, CDC |
| 9 | zbirna tabela OLTP vs OLAP sa izmerenim vrednostima | poređenje i zaključci |

## Izlazni fajlovi (`rezultati/`)

- `izvestaj.md` – sve tabele (šema, OLTP metrike, ACID provere, planovi pristupa,
  ETL, OLAP upiti sa SQL kodom, interferencija, inkrementalni ETL, sažetak)
- `rezultati.csv` – sirovi rezultati
- `olap_rezultati.txt` – kompletni rezultati svih OLAP upita (za tabele u radu)
- `grafik_oltp_latencija.png`, `grafik_interferencija.png`,
  `grafik_3nf_vs_star.png`, `grafik_olap_upiti.png`

## Primer rezultata (test okruženje: 4 CPU, PostgreSQL 16, podrazumevani parametri)

**OLTP** (4 klijenta, 15 s): 3.181 TPS, p50 0,82 ms, p95 2,83 ms; 2.619
ROLLBACK-ova zbog nedovoljne zalihe; 0 deadlock-a; sve ACID provere prošle.

**Obrasci pristupa:**

| upit | vreme | pročitano redova | blokova |
|---|---|---|---|
| OLTP: narudžbina po PK | 0,05 ms | 2 | 7 |
| OLAP: prihod po regionu/kategoriji/godini | 651 ms | 843.285 | 8.438 |

**Isto pitanje, različiti modeli:** 3NF (6 spajanja) 514 ms → star schema
(3 spajanja) 147 ms → materijalizovani pogled 1,1 ms; rezultati identični.

**Interferencija:** samo OLTP 3.211 TPS (p95 2,7 ms) → OLTP + 2 OLAP niti
1.341 TPS (p95 9,7 ms): **pad propusne moći 58%, latencija ×3,6**.

Zapažanja za rad:

- OLTP transakcija dodiruje nekoliko redova preko indeksa i traje delove
  milisekunde; analitički upit čita stotine hiljada redova i traje stotine ms –
  razlika od 4–5 redova veličine u broju pročitanih blokova.
- ROLLBACK zbog nedovoljne zalihe ne ostavlja „polovične“ narudžbine (atomičnost);
  `CHECK (stock >= 0)` + `FOR UPDATE` garantuju konzistentnost i pri konkurentnom
  radu; fiksni redosled zaključavanja (po `product_id`) sprečava deadlock.
- Zvezdasta šema je brža za analitiku (manje spajanja, mera je već izračunata), a
  materijalizovani pogled je brži za red veličine, ali mora da se osvežava.
- BRIN indeks nad `date_key` zauzima nekoliko desetina kB (B-tree nad
  `product_key` nekoliko MB), jer su činjenice učitane hronološki.
- Analitika pokrenuta direktno nad OLTP bazom drastično usporava transakcije –
  glavni argument za odvojeno skladište podataka, uz cenu kašnjenja podataka
  (korak 8).

Napomena: narudžbine koje generišu OLTP testovi nose današnji datum, pa su
vrednosti za tekući dan/mesec u OLAP upitima veće od istorijskog proseka.
