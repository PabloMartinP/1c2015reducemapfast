/*
 ============================================================================
 Name        : procesoJob.c
 Author      : 
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include <libgen.h>
#include <stdio.h>
#include <stdlib.h>
#include <commons/log.h>
#include <commons/collections/list.h>
#include <pthread.h>

#include "procesos.h"
#include "config_job.h"

int main(void) {
	jobConfig = config_create(FILE_CONFIG);

	printf("%s", JOB_IP_MARTA());

	//test conexion con marta
	conectar_con_marta();


	//finalizo el programa para que no intente conectar con el nodo
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
