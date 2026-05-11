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

typedef enum {InRun, InMonitor, InWant, InSection, InFinish} state_t;
extern state_t stan;
extern pthread_mutex_t stateMut;
/* zmiana stanu, obwarowana muteksem */
void changeState( state_t );
int max(int a, int b);

extern int lamport_clock;
extern pthread_mutex_t lamport_mutex;

#endif
