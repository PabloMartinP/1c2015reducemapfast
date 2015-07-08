/*
 ============================================================================
 Name        : procesoJob.c
 Author      : 
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include "procesoJob.h"

char FILE_LOG[1024] = "/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoJob/log.txt";

int contador_ftok = 0;
int main(int argc, char *argv[]) {
	jobConfig = config_create(FILE_CONFIG);
	logger = log_create(FILE_LOG, "JOB", true, LOG_LEVEL_TRACE);
	pthread_mutex_init(&mutex_log, NULL);


	//int i; /*      loop variables          */
	key_t shmkey; /*      shared memory key       */
	int shmid; /*      shared memory id        */
	//sem_t *sem; /*      synch semaphore         *//*shared */
	//pid_t pid; /*      fork pid                */
	int *p; /*      shared variable         *//*shared */
	//unsigned int n; /*      fork count              */
	unsigned int value; /*      semaphore value         */

	/* initialize a shared variable in shared memory */

	/*pthread_mutex_lock(&mutex);
	contador_ftok++;
	pthread_mutex_unlock(&mutex);*/

	//shmkey = ftok("/dev/null", 5); /* valid directory name and a number */
	shmkey = ftok("/dev/null", contador_ftok); /* valid directory name and a number */

	printf("shmkey for p = %d\n", shmkey);
	shmid = shmget(shmkey, sizeof(int), 0644 | IPC_CREAT);
	if (shmid < 0) { /* shared memory error check */
		perror("shmget\n");
		exit(1);
	}

	p = (int *) shmat(shmid, NULL, 0); /* attach p to shared memory */
	*p = 0;
	printf("p=%d is allocated in shared memory.\n\n", *p);
	///////////////////////////////////////////
	value = 0;
	/* initialize semaphores for shared processes */
	sem = sem_open("pSem", O_CREAT | O_EXCL, 0644, value);
	/* name of semaphore is "pSem", semaphore is reached using this name */
	sem_unlink("pSem");
	/* unlink prevents the semaphore existing forever */
	/* if a crash occurs during the execution         */
	printf ("semaphores initialized.\n\n");



	//test conexion con marta
	conectar_con_marta();


	log_trace(logger, "FIN TODO");

	/* shared memory detach */
	shmdt(p);
	shmctl(shmid, IPC_RMID, 0);

	/* cleanup semaphores */
	sem_destroy (sem);

	//finalizo el programa para que no intente conectar con el nodo
	log_destroy(logger);
	config_destroy(jobConfig);

	return EXIT_SUCCESS;
}




