#include "main.h"
#include "watek_glowny.h"
#include <limits.h>

#define REPAIR_TIME 1
#define DECISION_TIME 1

void mainLoop()
{
	srandom(rank);

	while (stan != FINISH)
	{
		switch (stan)
		{
		case WOLNY:
			sleep(DECISION_TIME);
			if (random() % 100 < 30)
			{
				debug("Potrzebuję naprawy!");
				int pref_dock = (rank % K_DOCKS) + 1;
				int min_load = INT_MAX;
				for (int d = 1; d <= K_DOCKS; d++) {
					int load = compute_load(d);
					if (load < min_load) min_load = load;
				}
				my_dock = (compute_load(pref_dock) == min_load) ? pref_dock : 0;
				if (!my_dock)
					for (int d = 1; d <= K_DOCKS; d++)
						if (compute_load(d) == min_load) { my_dock = d; break; }
				my_r = (random() % MAX_R) + 1;
				pthread_mutex_lock(&lamport_mutex);
				lamport_clock++;
				my_ts = lamport_clock;
				pthread_mutex_unlock(&lamport_mutex);
				pthread_mutex_lock(&stateMut);
				ack_count = 0;
				pthread_mutex_unlock(&stateMut);
				add_to_queue(rank, my_ts, my_r, my_dock);
				println("REQ (dock=%d, r=%d)", my_dock, my_r);
				for (int i = 0; i < size; i++)
				{
					if (i != rank)
					{
						packet_t req_pkt = { .ts = my_ts, .src = rank,
						                     .r = my_r, .dock = my_dock };
						MPI_Send(&req_pkt, 1, MPI_PAKIET_T, i, REQUEST, MPI_COMM_WORLD);
					}
				}
				changeState(ZADANIE);
				check_entry_conditions(M_MECHANICS);
			}
			break;
		case ZADANIE:
			pthread_mutex_lock(&stateMut);
			println("Czekam ACK (%d/%d)", ack_count, size - 1);
			while (stan == ZADANIE)
				pthread_cond_wait(&stateCond, &stateMut);
			pthread_mutex_unlock(&stateMut);
			break;
		case NAPRAWA:
			println("NAPRAWA w doku %d", my_dock);
			sleep(REPAIR_TIME);
			remove_from_queue(rank);
			pthread_mutex_lock(&lamport_mutex);
			lamport_clock++;
			pthread_mutex_unlock(&lamport_mutex);
			println("RELEASE z doku %d", my_dock);
			for (int i = 0; i < size; i++)
			{
				if (i != rank)
				{
					packet_t rel_pkt = { .r = my_r, .dock = my_dock };
					sendPacket(&rel_pkt, i, RELEASE);
				}
			}
			changeState(WOLNY);
			break;
		default:
			break;
		}
	}
}
