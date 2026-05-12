#include "main.h"
#include "util.h"
MPI_Datatype MPI_PAKIET_T;

/* 
 * w util.h extern state_t stan (czyli zapowiedź, że gdzieś tam jest definicja
 * tutaj w util.c state_t stan (czyli faktyczna definicja)
 */
state_t stan = WOLNY;

/* zamek wokół zmiennej współdzielonej między wątkami.
 * Zwróćcie uwagę, że każdy proces ma osobą pamięć, ale w ramach jednego
 * procesu wątki współdzielą zmienne - więc dostęp do nich powinien
 * być obwarowany muteksami
 */
pthread_mutex_t stateMut = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t  stateCond = PTHREAD_COND_INITIALIZER;

int lamport_clock = 0;
pthread_mutex_t lamport_mutex = PTHREAD_MUTEX_INITIALIZER;

int my_dock = 0;
int my_ts = 0;
int my_r = 1;
int ack_count = 0;

#define MAX_QUEUE_SIZE 256
request_entry_t local_queue[MAX_QUEUE_SIZE];
int queue_size = 0;
pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;

struct tagNames_t
{
    const char *name;
    int tag;
} tagNames[] = { { "pakiet aplikacyjny", APP_PKT }, { "finish", FINISH}, 
                { "potwierdzenie", ACK}, {"prośbę o sekcję krytyczną", REQUEST}, {"zwolnienie sekcji krytycznej", RELEASE} };

const char *const tag2string( int tag )
{
    for (int i=0; i <sizeof(tagNames)/sizeof(struct tagNames_t);i++) {
	if ( tagNames[i].tag == tag )  return tagNames[i].name;
    }
    return "<unknown>";
}
/* tworzy typ MPI_PAKIET_T
*/
void inicjuj_typ_pakietu()
{
    /* Stworzenie typu */
    /* Poniższe (aż do MPI_Type_commit) potrzebne tylko, jeżeli
       brzydzimy się czymś w rodzaju MPI_Send(&typ, sizeof(pakiet_t), MPI_BYTE....
    */
    /* sklejone z stackoverflow */
    int       blocklengths[NITEMS] = {1,1,1,1,1};
    MPI_Datatype typy[NITEMS] = {MPI_INT, MPI_INT, MPI_INT, MPI_INT, MPI_INT};

    MPI_Aint     offsets[NITEMS];
    offsets[0] = offsetof(packet_t, ts);
    offsets[1] = offsetof(packet_t, src);
    offsets[2] = offsetof(packet_t, data);
    offsets[3] = offsetof(packet_t, r);
    offsets[4] = offsetof(packet_t, dock);

    MPI_Type_create_struct(NITEMS, blocklengths, offsets, typy, &MPI_PAKIET_T);

    MPI_Type_commit(&MPI_PAKIET_T);
}

/* opis patrz util.h */
void sendPacket(packet_t *pkt, int destination, int tag)
{
    int freepkt=0;
    if (pkt==0) { pkt = malloc(sizeof(packet_t)); freepkt=1;}
    pkt->src = rank;

    pthread_mutex_lock( &lamport_mutex );
    lamport_clock++;
    pkt->ts = lamport_clock;
    pthread_mutex_unlock( &lamport_mutex );

    MPI_Send( pkt, 1, MPI_PAKIET_T, destination, tag, MPI_COMM_WORLD);
    debug("Wysyłam %s do %d\n", tag2string( tag), destination);
    if (freepkt) free(pkt);
}

void changeState( state_t newState )
{
    pthread_mutex_lock( &stateMut );
    if (stan==FINISH) {
        pthread_mutex_unlock( &stateMut );
        return;
    }
    stan = newState;
    pthread_cond_broadcast( &stateCond );
    pthread_mutex_unlock( &stateMut );
}

void check_entry_conditions(int total_mechanics)
{
    pthread_mutex_lock( &stateMut );
    if (stan != ZADANIE) {
        pthread_mutex_unlock( &stateMut );
        return;
    }
    int ac = ack_count;
    pthread_mutex_unlock( &stateMut );

    if (ac != size - 1) return;


    pthread_mutex_lock(&queue_mutex);


    int first = 0;
    for (int i = 0; i < queue_size; i++)
    {
        if (local_queue[i].dock == my_dock)
        {
            first = (local_queue[i].process_id == rank &&
                     local_queue[i].ts == my_ts);
            break;
        }
    }


    int sum = 0;
    for (int i = 0; i < queue_size; i++)
    {
        if (local_queue[i].process_id == rank && local_queue[i].ts == my_ts)
            break;
        sum += local_queue[i].r;
    }
    int enough = (sum + my_r <= total_mechanics);

    pthread_mutex_unlock(&queue_mutex);

    if (first && enough)
        changeState(NAPRAWA);
}

int max(int a, int b)
{
    if (a>b)
    {
        return a;
    }
    return b;
}


static int compare_requests(const request_entry_t *a, const request_entry_t *b)
{
    if (a->ts != b->ts)
        return a->ts - b->ts;
    return a->process_id - b->process_id;
}


void add_to_queue(int process_id, int ts, int r, int dock)
{
    pthread_mutex_lock(&queue_mutex);

    if (queue_size >= MAX_QUEUE_SIZE)
    {
        pthread_mutex_unlock(&queue_mutex);
        return;
    }


    local_queue[queue_size].process_id = process_id;
    local_queue[queue_size].ts = ts;
    local_queue[queue_size].r = r;
    local_queue[queue_size].dock = dock;
    queue_size++;


    for (int i = queue_size - 1; i > 0; i--)
    {
        if (compare_requests(&local_queue[i], &local_queue[i - 1]) < 0)
        {
            request_entry_t tmp = local_queue[i];
            local_queue[i] = local_queue[i - 1];
            local_queue[i - 1] = tmp;
        }
        else
        {
            break;
        }
    }

    pthread_mutex_unlock(&queue_mutex);
}


void remove_from_queue(int process_id)
{
    pthread_mutex_lock(&queue_mutex);

    for (int i = 0; i < queue_size; i++)
    {
        if (local_queue[i].process_id == process_id)
        {
            for (int j = i; j < queue_size - 1; j++)
            {
                local_queue[j] = local_queue[j + 1];
            }
            queue_size--;
            break;
        }
    }

    pthread_mutex_unlock(&queue_mutex);
}


int compute_load(int dock)
{
    pthread_mutex_lock(&queue_mutex);
    int count = 0;
    for (int i = 0; i < queue_size; i++)
    {
        if (local_queue[i].dock == dock)
        {
            count++;
        }
    }
    pthread_mutex_unlock(&queue_mutex);
    return count;
}