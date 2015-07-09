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



	//sem_t sem;
	sem_init(&sem, 0, 0);


	//test conexion con marta
	conectar_con_marta();


	log_trace(logger, "FIN TODO");


	/* cleanup semaphores */
	sem_destroy (&sem);

	//desconecto al nodo
	int socket = client_socket("192.168.1.39", 6001);
	t_msg* msg = argv_message(123, 0);
	enviar_mensaje(socket, msg);
	destroy_message(msg);
	close(socket);

	//finalizo el programa para que no intente conectar con el nodo
	log_destroy(logger);
	config_destroy(jobConfig);

	return EXIT_SUCCESS;
}




