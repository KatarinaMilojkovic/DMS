# Full vs Incremental backup (pgBackRest) - rezultati

Datum: 30.09.2026 01:58 · pgBackRest 2.58.0 · postgres (PostgreSQL) 18.6 (Ubuntu 18.6-0ubuntu0.26.04.1) · kompresija: lz4 · veličina baze: 492.5 MB

## 1. Test podaci

| tabela | redova | veličina | fajl |
|---|---|---|---|
| big_static | 1,800,000 | 242.5 MB | base/5/16384 |
| hot_table | 900,000 | 135.2 MB | base/5/16392 |
| event_log | 300,000 | 42.6 MB | base/5/16400 |

## 2. Scenario

1. **FULL** - kompletna kopija klastera.
2. **INCR #1** - posle INSERT-a u `event_log`.
3. **INCR #2** - posle UPDATE-a 1% redova u `hot_table`.
4. Restore INCR #2 u novi direktorijum + verifikacija kontrolnih suma.
5. **DIFF** - sve promene od FULL-a; **INCR #3** - mala promena posle DIFF-a.
6. PITR: restore point -> `DROP TABLE hot_table` -> restore do restore point-a.
7. Retention: novi FULL + `expire --repo1-retention-full=1`.

## 3. Backup-i

| backup | repo | tip | trajanje | veličina baze | kopirano (delta) | % baze | u repozitorijumu | zavisi od |
|---|---|---|---|---|---|---|---|---|
| FULL | 1 (file) | FULL | 3.18 s | 507.6 MB | 507.6 MB | 100.0% | 180.7 MB | - |
| FULL | 2 (block) | FULL | 1.54 s | 507.6 MB | 507.6 MB | 100.0% | 181.0 MB | - |
| INCR #1 | 1 (file) | INCR | 0.83 s | 509.2 MB | 51.0 MB | 10.0% | 18.7 MB | 20260930-015817F |
| INCR #1 | 2 (block) | INCR | 0.93 s | 509.2 MB | 50.8 MB | 10.0% | 0.8 MB | 20260930-015820F |
| INCR #2 | 1 (file) | INCR | 1.13 s | 510.8 MB | 156.1 MB | 30.6% | 54.8 MB | 20260930-015817F_20260930-015825I |
| INCR #2 | 2 (block) | INCR | 0.79 s | 510.8 MB | 156.1 MB | 30.6% | 1.1 MB | 20260930-015820F_20260930-015826I |
| DIFF | 1 (file) | DIFF | 0.83 s | 510.8 MB | 207.1 MB | 40.6% | 73.5 MB | 20260930-015817F |
| DIFF | 2 (block) | DIFF | 0.76 s | 510.8 MB | 206.9 MB | 40.5% | 1.9 MB | 20260930-015820F |
| INCR #3 | 1 (file) | INCR | 0.84 s | 510.9 MB | 50.9 MB | 10.0% | 18.7 MB | 20260930-015817F_20260930-015841D |
| INCR #3 | 2 (block) | INCR | 0.92 s | 510.9 MB | 50.9 MB | 10.0% | 0.1 MB | 20260930-015820F_20260930-015842D |

- *kopirano (delta)* - nekompresovana količina podataka koju je backup zaista kopirao;
- *u repozitorijumu* - zauzeće tog backup-a u repozitorijumu (posle kompresije);
- *zavisi od* - prethodni backup u lancu (potreban za restore).

## 4. Restore testovi

| test | pgbackrest restore | start + WAL recovery | verifikacija |
|---|---|---|---|
| Restore INCR #2 (20260930-015817F_20260930-015830I) | 2.52 s | 0.41 s | OK |
| PITR do 'pre_katastrofa' (--delta) | 0.66 s | 0.41 s | OK |

## 5. Retention

Posle novog FULL backup-a i `expire` sa `repo1-retention-full=1` obrisano je 5 backup-a: `20260930-015817F`, `20260930-015817F_20260930-015825I`, `20260930-015817F_20260930-015830I`, `20260930-015817F_20260930-015841D`, `20260930-015817F_20260930-015846I`.

## 6. Zapažanja

- INCR #2 je nastao posle izmene 1% redova `hot_table`, a kopirao je 156.1 MB (31% full backup-a) - pgBackRest inkrement je na nivou **fajla**, a `hot_table` je jedan fajl od 136.6 MB.
- Sa block incremental-om (repo2) isti INCR #2 u repozitorijumu zauzima 1.1 MB umesto 54.8 MB - čuvaju se samo izmenjeni blokovi.
- DIFF kopira sve promene od FULL-a (raste vremenom), ali restore zahteva samo FULL + DIFF.
- INCR je najmanji i najbrži, ali restore zahteva ceo lanac FULL -> ... -> INCR.

## 7. Fajlovi

- `backupi.csv` - sirovi rezultati
- `pgbackrest_info.txt` - izlaz `pgbackrest info`
