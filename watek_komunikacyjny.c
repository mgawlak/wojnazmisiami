#include "main.h"
#include "watek_komunikacyjny.h"

void *startKomWatek(void *ptr)
{
    MPI_Status status;
    packet_t pakiet;

    while (stan != FINISH)
    {
        MPI_Recv(&pakiet, 1, MPI_PAKIET_T, MPI_ANY_SOURCE, MPI_ANY_TAG,
                 MPI_COMM_WORLD, &status);

        pthread_mutex_lock(&lamport_mutex);
        lamport_clock = max(lamport_clock, pakiet.ts) + 1;
        pthread_mutex_unlock(&lamport_mutex);

        int dock = pakiet.dock;
        int r    = pakiet.r;

        switch (status.MPI_TAG)
        {
        case REQUEST:
            debug("REQUEST od %d (dok=%d, r=%d)", pakiet.src, dock, r);
            add_to_queue(pakiet.src, pakiet.ts, r, dock);
            sendPacket(NULL, pakiet.src, ACK);
            check_entry_conditions(M_MECHANICS);
            break;

        case ACK:
            debug("ACK od %d", pakiet.src);
            pthread_mutex_lock(&stateMut);
            ack_count++;
            pthread_mutex_unlock(&stateMut);
            check_entry_conditions(M_MECHANICS);
            break;

        case RELEASE:
            debug("RELEASE od %d (dok=%d, r=%d)", pakiet.src, dock, r);
            remove_from_queue(pakiet.src);
            check_entry_conditions(M_MECHANICS);
            break;

        default:
            break;
        }
    }
}
