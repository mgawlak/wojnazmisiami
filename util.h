#ifndef UTILH
#define UTILH
#include "main.h"

#define K_DOCKS     6
#define M_MECHANICS 10
#define MAX_R       4

/* typ pakietu */
typedef struct {
    int ts;
    int src;
    int data;
    int r;
    int dock;
} packet_t;

#define NITEMS 5

/* Typy wiadomości */
/* TYPY PAKIETÓW */
#define ACK     1
#define REQUEST 2
#define RELEASE 3
#define APP_PKT 4

extern MPI_Datatype MPI_PAKIET_T;
void inicjuj_typ_pakietu();

/* wysyłanie pakietu, skrót: wskaźnik do pakietu (0 oznacza stwórz pusty pakiet), do kogo, z jakim typem */
void sendPacket(packet_t *pkt, int destination, int tag);

void add_to_queue(int process_id, int ts, int r, int dock);
void remove_from_queue(int process_id);
int compute_load(int dock);

typedef enum
{
    WOLNY,
    ZADANIE,
    NAPRAWA,
    FINISH
} state_t;
extern state_t stan;
extern pthread_mutex_t stateMut;
extern pthread_cond_t stateCond;

void changeState( state_t );

void check_entry_conditions(int total_mechanics);
int max(int a, int b);

extern int lamport_clock;
extern pthread_mutex_t lamport_mutex;


typedef struct
{
    int process_id;
    int ts;
    int r;
    int dock;
} request_entry_t;


extern int my_dock;
extern int my_ts;
extern int my_r;
extern int ack_count;

#endif
