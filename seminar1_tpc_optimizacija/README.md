# Seminarski 1 – Optimizacija upita primenom TPC-like benchmarking pristupa

Skripta `tpc_optimizacija.py` sprovodi benchmark-vođenu optimizaciju upita: isti
skup upita (izveden iz TPC-H) meri se posle svake faze optimizacije, tako da je
efekat svake tehnike izmeren, a ne pretpostavljen.

## Pokretanje

```bash
pip install psycopg2-binary matplotlib
python tpc_optimizacija.py --host localhost --user postgres --password postgres
```

| parametar | podrazumevano | značenje |
|---|---|---|
| `--scale` | `0.3` | TPC faktor skaliranja SF (0.3 ≈ 1,8M redova `lineitem`, ≈ 350 MB). Za brzi test `0.1`, za izraženije efekte `1.0` |
| `--runs` | `3` | broj merenja po upitu posle jednog zagrevanja (uzima se medijana) |
| `--timeout` | `60` | `statement_timeout` u sekundama – štiti od upita koji bi trajali satima |
| `--streams` | `1,2,4` | broj paralelnih tokova u throughput testu (`0` = preskoči) |
| `--db` | `tpc_demo` | baza koja se kreira (postojeća baza istog imena se briše!) |
| `--keep` | – | zadrži bazu posle izvršavanja (za sopstvene eksperimente u `psql`) |
| `--out` | `./rezultati` | direktorijum za izveštaj, CSV, grafike i planove |

Trajanje: ~4–5 min za SF 0.3 (najviše odlazi na baseline fazu i Q17 koji prekoračuje timeout).

## Koraci skripte i veza sa teorijom

| korak | šta radi | teorijsko poglavlje |
|---|---|---|
| 1 | kreira bazu, ispisuje verziju i podešavanja servera | okruženje benchmarka, ponovljivost |
| 2 | kreira TPC-H-like šemu (8 tabela) **bez** ključeva i indeksa, autovacuum isključen | TPC-H šema, polazno (neoptimizovano) stanje |
| 3 | generiše podatke u bazi (`generate_series` + `setseed`) za zadati SF | faktor skaliranja, kardinalnosti, reproduktivnost |
| 4 | **Faza 0 – baseline**: meri svih 9 upita; poredi procenu broja redova optimizatora sa stvarnim | metodologija merenja (zagrevanje, medijana, timeout) |
| 5 | **Faza 1 – `ANALYZE`**: statistika (histogrami, MCV, n_distinct) → procene postaju tačne | optimizator zasnovan na troškovima (cost-based), statistika |
| 6 | **Faza 2 – indeksi**: PK, FK, indeksi nad FK kolonama, indeks po datumu, pokrivajući (`INCLUDE`) i kompozitni indeks + `VACUUM` | B-tree indeksi, Index Scan / Index Only Scan / Bitmap Scan, visibility map |
| 7 | **Faza 3 – prepisivanje upita**: Q14 `date_trunc(...) = ...` → opseg (sargable); Q17 korelisani podupit → JOIN sa preagregacijom | sargable predikati, dekorelacija podupita |
| 8 | **Faza 4 – konfiguracija**: `work_mem`, `max_parallel_workers_per_gather`, `random_page_cost`, `jit` | memorija za sort/hash (spill na disk), paralelno izvršavanje |
| 9 | **Faza 5 – materijalizovani pogled** za Q1 (pre-agregacija po danu) | materijalizovani pogledi, cena osvežavanja |
| 10 | **Throughput test**: 1, 2, 4 paralelna toka izvršavaju sve upite u permutovanom redosledu | TPC-H Power i Throughput test, QphH |
| 11 | zbirna tabela, geometrijska sredina, Power-like metrika, regresije, „pre/posle“ planovi, cena optimizacije (prostor i vreme) | analiza rezultata |

Faze su **kumulativne** – svaka zadržava sve prethodne optimizacije.

### Upiti

Q1, Q3, Q5, Q6, Q10, Q12, Q14, Q17 i Q18 iz TPC-H specifikacije, sa
podrazumevanim parametrima. Dva upita su **namerno** napisana neoptimalno (a
semantički ekvivalentno), da bi faza prepisivanja imala šta da pokaže:

- **Q14** filtrira sa `date_trunc('month', l_shipdate) = DATE '1995-09-01'` –
  funkcija nad kolonom sprečava upotrebu indeksa; prepisana verzija koristi opseg
  `l_shipdate >= '1995-09-01' AND l_shipdate < '1995-10-01'`.
- **Q17** (original iz TPC-H) sadrži korelisani podupit koji se izvršava za svaki
  red; prepisana verzija računa prosek jednom po delu (`GROUP BY` u CTE-u).

Skripta za svaki upit u svakoj fazi proverava da je **rezultat isti** kao u
originalu (optimizacija ne sme da promeni rezultat).

## Izlazni fajlovi (`rezultati/`)

- `izvestaj.md` – sve tabele spremne za rad (podaci, procene kardinalnosti,
  vremena po fazama, pročitani blokovi, cena optimizacije, throughput, regresije)
- `rezultati.csv` – sirova merenja (medijana/min/max, blokovi, temp blokovi)
- `planovi/fazaN_QX.txt` – `EXPLAIN (ANALYZE, BUFFERS)` plan svakog upita u
  svakoj fazi (za ilustracije u radu: Seq Scan → Index Only Scan, spill na disk…)
- `grafik_upiti_po_fazama.png`, `grafik_geomean_po_fazama.png`, `grafik_throughput.png`

## Primer rezultata (test okruženje: 4 CPU, PostgreSQL 16, SF 0.3)

| upit | F0 Baseline | F1 ANALYZE | F2 Indeksi | F3 Prepisani | F4 Konfig. | F5 Mat. pogled |
|---|---|---|---|---|---|---|
| Q1 | 1217 ms | 665 ms | 596 ms | 606 ms | 444 ms | 2 ms |
| Q14 | 339 ms | 216 ms | 192 ms | 26 ms | 23 ms | 24 ms |
| Q17 | ≥ 60 s | ≥ 60 s | 204 ms | 163 ms | 90 ms | 86 ms |
| Q18 | 3456 ms | 1395 ms | 695 ms | 693 ms | 1047 ms | 1075 ms |
| **geomean** | 702 ms | 422 ms | 167 ms | 129 ms | 108 ms | 62 ms |

Zapažanja koja vredi prokomentarisati u radu:

- bez statistike optimizator greši i do ~150 puta u proceni broja redova, pa bira
  loše planove (sort umesto hash agregacije, spill na disk – kolona `blokovi_temp`);
- Q17 sa korelisanim podupitom je neupotrebljiv bez indeksa nad `l_partkey`
  (kvadratna složenost), a indeks ga ubrzava stotinama puta;
- indeks nad `l_shipdate` sam po sebi **ne pomaže** Q14 dok se upit ne prepiše u
  sargable oblik (F2 → F3);
- Q18 u fazi 4 može postati **sporiji**: sa većim `work_mem` optimizator bira
  `HashAggregate` bez paralelizma. Skripta takve slučajeve automatski prijavljuje
  kao regresije – svaka promena konfiguracije mora da se proveri benchmarkom;
- materijalizovani pogled daje najveće ubrzanje, ali po cenu prostora i
  periodičnog osvežavanja (vreme `REFRESH` je u tabeli cene optimizacije).

Tačni brojevi zavise od hardvera, ali odnosi između faza su stabilni.

## Napomena o TPC

Ovo je **TPC-like** benchmark: šema, distribucije i upiti su izvedeni iz TPC-H
specifikacije, ali podaci se ne generišu zvaničnim `dbgen` alatom i rezultati
nisu auditovani, pa se ne smeju predstavljati kao zvanični TPC-H rezultati
(metrike su zato nazvane „Power-like“ i „Throughput-like“).
