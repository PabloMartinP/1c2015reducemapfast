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


int main(int argc, char *argv[]) {
	jobConfig = config_create(FILE_CONFIG);
	logger = log_create(FILE_LOG, "JOB", true, LOG_LEVEL_TRACE);
	pthread_mutex_init(&mutex_log, NULL);

	//test conexion con marta
	conectar_con_marta();


	log_trace(logger, "FIN TODO");
	//finalizo el programa para que no intente conectar con el nodo
	log_destroy(logger);
	config_destroy(jobConfig);
	return 0;


	/////////////////////////////////////////////////
	//TEST CONEXION CON NODO
	int socketjob = client_socket("127.0.0.1", 6001);

	t_msg* msg;
	msg = string_message(JOB_HOLA, "hola soy job", 0);
	enviar_mensaje(socketjob, msg);
	destroy_message(msg);


	config_destroy(jobConfig);
	////////////////////////////////////////////////////////
	return EXIT_SUCCESS;
}




