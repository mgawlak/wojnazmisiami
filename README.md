# wojnazmisiami

Implementacja zegarów Lamporta i algorytmu wzajemnego wykluczania w MPI (Programowanie Równoległe i Rozproszone).

Projekt oparty na szkielecie wielowątkowym MPI — wątek główny + wątek komunikacyjny.

## Struktura
| Plik | Opis |
|------|------|
| `main.c` / `main.h` | Punkt wejścia, zmienne współdzielone, makra debug |
| `util.c` / `util.h` | Wysyłanie wiadomości, inicjalizacja MPI |
| `watek_glowny.c` | Główna pętla programu |
| `watek_komunikacyjny.c` | Wątek odbierający wiadomości MPI |

## Stack
C · MPI · POSIX Threads

## Kompilacja
```bash
mpicc -o main main.c util.c watek_glowny.c watek_komunikacyjny.c -lpthread
mpirun -np 4 ./main
```