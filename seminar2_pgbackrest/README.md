# Seminarski 2 – Full vs Incremental backup korišćenjem pgBackRest alata

Skripta `pgbackrest_full_vs_incr.py` pravi **izolovanu laboratoriju** – sopstveni
PostgreSQL klaster (`initdb`) sa sopstvenim pgBackRest repozitorijumom u jednom
direktorijumu – i nad njom izvodi scenario FULL → INCR → INCR → restore → DIFF →
INCR → PITR → retention. Postojeći PostgreSQL serveri se **ne diraju**.

## Preduslovi

- Linux ili macOS (pgBackRest ne postoji za Windows – koristiti **WSL**)
- PostgreSQL server binarni fajlovi (`initdb`, `pg_ctl`; skripta ih sama nalazi u
  `/usr/lib/postgresql/*/bin`, ili se navode sa `--pg-bin`)
- pgBackRest (≥ 2.46 za opciju `--block-incr`)
- `pip install psycopg2-binary matplotlib`
- pokretanje kao **običan korisnik** – PostgreSQL odbija da radi kao root

```bash
# Ubuntu / Debian / WSL
sudo apt install postgresql pgbackrest python3-pip
pip install psycopg2-binary matplotlib

python3 pgbackrest_full_vs_incr.py --block-incr
# ako ste root (npr. u kontejneru):
sudo -u postgres python3 pgbackrest_full_vs_incr.py --lab /tmp/pgbr_lab --out /tmp/pgbr_rezultati --block-incr
```

| parametar | podrazumevano | značenje |
|---|---|---|
| `--scale` | `500` | približna veličina test podataka u MB (veće vrednosti → izraženija razlika u trajanju) |
| `--block-incr` | – | dodatni repozitorijum `repo2` sa **block incremental** backup-om; svi backup-i se prave u oba repozitorijuma radi direktnog poređenja |
| `--compress` | `lz4` | kompresija u repozitorijumu (`lz4`, `zst`, `gz`, `bz2`, `none`) |
| `--lab` | `./pgbr_lab` | direktorijum laboratorije (klaster, repozitorijumi, logovi) |
| `--port` | `5433` | port lab klastera (restore test koristi `port+1`); klaster sluša samo na unix socketu |
| `--keep-running` | – | ostavi lab klaster pokrenut na kraju (za ručno isprobavanje `pgbackrest` komandi) |
| `--cleanup` | – | na kraju obriši ceo lab direktorijum |
| `--out` | `./rezultati` | direktorijum za izveštaj, CSV i grafike |

Trajanje: ~2 min za 500 MB. Potreban prostor na disku: ~3× `--scale`.

## Koraci skripte i veza sa teorijom

| korak | šta radi | teorijsko poglavlje |
|---|---|---|
| 1 | proverava pgBackRest, PostgreSQL, korisnika | arhitektura pgBackRest-a |
| 2 | `initdb --data-checksums` u lab direktorijumu | kontrolne sume stranica (pgBackRest ih proverava pri backup-u) |
| 3 | `wal_level=replica`, `archive_mode=on`, `archive_command = pgbackrest archive-push`; generiše `pgbackrest.conf`; `stanza-create`, `check` | WAL, kontinuirano arhiviranje, stanza, repozitorijum |
| 4 | puni tri tabele: `big_static` (nikad se ne menja), `hot_table` (male izmene), `event_log` (samo INSERT); `VACUUM FREEZE` | fizička organizacija: svaka tabela je fajl (segmenti do 1 GB) |
| 5 | **FULL** backup | pun backup – nezavisan, najveći, najsporiji |
| 6 | INSERT u `event_log` → **INCR #1** | inkrementalni backup – samo fajlovi promenjeni od **prethodnog** backup-a |
| 7 | UPDATE 1% redova `hot_table` → **INCR #2** | granularnost **fajla**: ceo fajl tabele se kopira iako je promenjen mali deo |
| 8 | restore INCR #2 u novi direktorijum (`--set`, `--type=immediate`), start, poređenje md5 kontrolnih suma | restore iz lanca FULL → INCR1 → INCR2, verifikacija backup-a |
| 9 | **DIFF**, pa **INCR #3** | diferencijalni backup – sve od poslednjeg FULL-a; kraći lanac za restore |
| 10 | `pg_create_restore_point` → `DROP TABLE` → `restore --delta --type=name` | point-in-time recovery (PITR), timeline, delta restore |
| 11 | novi FULL + `expire --repo1-retention-full=1` | politika zadržavanja; INCR/DIFF se brišu zajedno sa svojim FULL-om |
| 12 | zbirna tabela, grafici, izveštaj | poređenje i zaključci |

## Izlazni fajlovi (`rezultati/`)

- `izvestaj.md` – tabele backup-a (trajanje, kopirano, zauzeće u repozitorijumu,
  zavisnosti), restore testova i retention-a, sa zapažanjima
- `backupi.csv` – sirovi podaci (uključujući pgBackRest labele i lanac referenci)
- `pgbackrest_info.txt` – izlaz `pgbackrest info` pre i posle expire-a
- `grafik_velicina_backupa.png`, `grafik_trajanje_backupa.png`

Konfiguracija (`pgbr_lab/pgbackrest.conf`) i logovi (`pgbr_lab/log/`) ostaju u
lab direktorijumu i mogu se citirati u radu.

## Primer rezultata (test okruženje: pgBackRest 2.50, PostgreSQL 16, `--scale 1000 --block-incr`)

| backup | trajanje | kopirano | % baze | u repo1 (file-level) | u repo2 (block) | zavisi od |
|---|---|---|---|---|---|---|
| FULL | 7.1 s | 991.8 MB | 100% | 357.0 MB | 357.5 MB | – |
| INCR #1 (INSERT) | 2.5 s | 101.7 MB | 10.2% | 37.3 MB | 1.6 MB | FULL |
| INCR #2 (UPDATE 1%) | 3.5 s | 312.2 MB | 31.3% | 109.6 MB | 2.2 MB | INCR #1 |
| DIFF | 4.5 s | 413.9 MB | 41.5% | 146.9 MB | 3.7 MB | FULL |
| INCR #3 | 2.8 s | 101.6 MB | 10.2% | 37.3 MB | 0.1 MB | DIFF |

Restore INCR #2 (lanac od 3 backup-a) i PITR posle `DROP TABLE` – oba verifikovana
md5 kontrolnim sumama.

Zapažanja za rad:

- **INCR #2**: izmenjen je 1% redova, a kopirano je 31% baze – pgBackRest
  (file-level) kopira ceo fajl `hot_table` (i njenog indeksa), jer je fajl
  promenjen. Veličina inkrementa zavisi od broja **promenjenih fajlova**, ne redova.
- **Block incremental** (repo2) čuva samo izmenjene blokove: isti INCR #2 zauzima
  ~2 MB umesto ~110 MB.
- **DIFF** raste sa svakom promenom od poslednjeg FULL-a, ali za restore su
  potrebna samo dva backup-a (FULL + DIFF); lanac inkrementala je duži.
- Trajanje malih backup-a je dominantno **fiksni trošak** (checkpoint, start/stop
  backup-a, arhiviranje WAL-a); razlika u trajanju raste sa veličinom baze.
- `--delta` restore (PITR) je brži od restore-a u prazan direktorijum jer
  prepisuje samo fajlove koji se razlikuju.
- `expire` briše FULL zajedno sa svim INCR/DIFF backup-ima koji zavise od njega.

## Napomene

- `autovacuum = off` je podešen **samo** u laboratoriji, da bi eksperiment bio
  deterministički (autovacuum bi menjao fajlove između backup-a). U produkciji
  autovacuum mora ostati uključen.
- Ako skripta bude prekinuta, sledeće pokretanje samo zaustavlja stari lab klaster
  i briše lab direktorijum (prepoznaje ga po fajlu `.pgbr_lab`).
