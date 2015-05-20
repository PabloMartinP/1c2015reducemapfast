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

typedef struct {
	char ip[15];
	int puerto;
}t_map;

void funcionMapping(t_map*);
void crearHiloMapper();

int main(int argc, char *argv[]) {
	jobConfig = config_create(FILE_CONFIG);

	printf("%s", JOB_IP_MARTA());

	//test conexion con marta
	conectar_con_marta();

	//finalizo el programa para que no intente conectar con el nodo

	crearHiloMapper();

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



void funcionMapping(t_map* map){
	printf("%s:%d\n", map->ip, map->puerto);
}

void crearHiloMapper(){
	pthread_t idHilo;

	t_map* map = malloc(sizeof(t_map));
	strcpy(map->ip, "127.0.0.1");
	map->puerto = 1234;

	pthread_create(&idHilo, NULL, (void*)funcionMapping, (void*)map); //que párametro  ponemos??
	pthread_join(idHilo, NULL); //el proceso espera a que termine el hilo
	free(map);
}

