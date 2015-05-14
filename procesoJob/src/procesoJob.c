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
#include <commons/config.h>
#include <commons/collections/list.h>
#include <pthread.h>
#include <util.h>


char FILE_CONFIG [1024] = "/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoJob/jobConfig.txt";

int main(void) {
	t_config* jobConfig = NULL;
	jobConfig = config_create(FILE_CONFIG);

	printf("%s", config_get_string_value(jobConfig, "IP_MARTA"));
	int socketjob = client_socket("127.0.0.1", 6001);

	t_msg* msg;
	msg = string_message(JOB_HOLA, "hola soy job", 0);
	enviar_mensaje(socketjob, msg);
	destroy_message(msg);

	config_destroy(jobConfig);
	return EXIT_SUCCESS;
}
