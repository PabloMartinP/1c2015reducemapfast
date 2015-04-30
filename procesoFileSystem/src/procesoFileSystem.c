/*
 ============================================================================
 Name        : procesoFileSystem.c
 Author      : 
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <commons/string.h>
#include <strings.h>
#include <commons/log.h>
#include <commons/config.h>
#include <unistd.h>
#include <commons/collections/list.h>

#include <pthread.h>

#include <util.h>
#include "consola.h"
#include <nodo.h>


#define FILE_CONFIG "/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoFileSystem/config.txt"
#define FILE_LOG "/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoFileSystem/log.txt"


t_log* logger;
t_config* config;

t_list* nodos;
t_list* nodos_nuevos; //los nodos que estan disponibles pero estan agregados al fs


void nuevosNodos();
void procesar_mensaje_nodo(int i, t_msg* msg);
void inicializar();
void fin();
void nodo_agregar_al_fs();

int main(void) {

	inicializar();

	iniciar_consola();

	return EXIT_SUCCESS;
}

void nodo_destroy(t_nodo* nodo){
	free_null(nodo->ip);

}

void nodo_agregar_al_fs(int id_nodo){
	//busco el id_nodo en la lista de nodos_nuevos
	bool buscar_nodo_nuevo_por_id(t_nodo* nodo){
		return nodo->identificador == id_nodo;
	}
	//busco el nodo
	t_nodo* nodo;
	if((nodo = list_find(nodos_nuevos, (void*)buscar_nodo_nuevo_por_id)) == NULL){
		printf("El nodo ingresado no existe: %d\n", id_nodo);
		return;
	}

	//loborro de la lista de nodos nuevos
	list_remove_by_condition(nodos_nuevos, (void*)buscar_nodo_nuevo_por_id);


	//lo paso a la lista de nodos activos del fs
	list_add(nodos, nodo);
	log_info(logger, "El nodo %d fue agregado al fs. %s:%d", nodo->identificador, nodo->ip, nodo->puerto);


}
void iniciar_consola() {
	char comando[COMMAND_MAX_SIZE];
	printf("inicio consola\nIngresar comandos  \n");

	bool fin = false;
	while (!fin) {
		fgets(comando, COMMAND_MAX_SIZE, stdin);

		 char** input_user= string_split(comando, " ");

		switch (getComando(input_user[0])) {
		case NODO_AGREGAR:
			printf("comando ingresado: agregar nodo\n");
			int id_nodo = atoi(input_user[1]);

			nodo_agregar_al_fs(id_nodo);

			//
			break;
		case NODO_ELIMINAR:
			printf("comando ingresado: elimnar nodo\n");

			break;
		case DIRECTORIO_CREAR:
			printf("crear directorio\n");

			directorio_crear(comando);
			break;
		case FORMATEAR:
			formatear();
			break;
		case SALIR:
			printf("comando ingresado: salir\n");
			fin = true;
			break;
		default:
			printf("comando desconocido\n");
			break;
		}

	}
}

void fin() {
	log_destroy(logger);
	config_destroy(config);
	printf("bb world!!!!");

}

void inicializar() {
	logger = log_create(FILE_LOG, "FileSystem", true, LOG_LEVEL_INFO);
	config = config_create(FILE_CONFIG);

	/*
	 * ABRO UN THREAD EL SERVER PARA QUE SE CONECTEN NODOS
	 */

	pthread_t th_nuevosNodos;
	if (pthread_create(&th_nuevosNodos, NULL, (void*) nuevosNodos, NULL) != 0) {
		perror("pthread_create - th_nuevosNodos");
		exit(1);
	}
	//espera a que termine el nodo: bloquea
	//pthread_join(th_nuevosNodos, NULL);
	//pthread_detach(th_nuevosNodos);

	nodos = list_create();
	nodos_nuevos = list_create();
}

void nuevosNodos() {
	fd_set master, read_fds;
	int fdNuevoNodo, fdmax, newfd;
	int i;

	if((fdNuevoNodo = server_socket(config_get_int_value(config, "PUERTO_LISTEN")))<0){
		handle_error("error al iniciar  server");
	}


	FD_ZERO(&master); // borra los conjuntos maestro y temporal
	FD_ZERO(&read_fds);
	FD_SET(fdNuevoNodo, &master);

	fdmax = fdNuevoNodo; // por ahora el maximo

	log_info(logger, "inicio thread eschca de nuevos nodos");
	// bucle principal
	for (;;) {
		read_fds = master; // cópialo
		if (select(fdmax + 1, &read_fds, NULL, NULL, NULL) == -1) {
			perror("select");
			exit(1);
		}

		// explorar conexiones existentes en busca de datos que leer
		for (i = 0; i <= fdmax; i++) {
			if (FD_ISSET(i, &read_fds)) { // ¡¡tenemos datos!!
				if (i == fdNuevoNodo) {	// gestionar nuevas conexiones
					//char * ip;
					//newfd = accept_connection_and_get_ip(fdNuevoNodo, &ip);
					newfd = accept_connection(fdNuevoNodo);
					//printf("nueva conexion desde IP: %s\n", ip);
					FD_SET(newfd, &master); // añadir al conjunto maestro
					if (newfd > fdmax) { // actualizar el máximo
						fdmax = newfd;
					}

				} else { // gestionar datos de un cliente ya conectado
					t_msg *msg = recibir_mensaje(i);
					if (msg == NULL) {
						/* Socket closed connection. */
						//int status = remove_from_lists(i);
						close(i);
						FD_CLR(i, &master);
					} else {
						procesar_mensaje_nodo(i, msg);

					}

				} //fin else procesar mensaje nodo ya conectado
			}
		}
	}

}




void procesar_mensaje_nodo(int i, t_msg* msg) {
	//leer el msg recibido
	//print_msg(msg);

	switch (msg->header.id) {
	case NODO_CONECTAR_CON_FS: //primer mensaje del nodo
		destroy_message(msg);
		msg = string_message(FS_NODO_QUIEN_SOS, "", 0);
		enviar_mensaje(i, msg);
		destroy_message(msg);
		msg = recibir_mensaje(i);
		//print_msg(msg);
		t_nodo* nodo= nodo_new(msg->stream, (uint16_t)msg->argv[0], (bool)msg->argv[1]);//0 puerto, 1 si es nuevo o no
		//le asigno un nombre
		nodo->identificador = get_id_nodo_nuevo();

		//agrego el nodo a la lista de nodos nuevos
		list_add(nodos_nuevos, (void*)nodo);

		log_info(logger, "se conecto el nodo %d,  %s:%d | %s",nodo->identificador, nodo->ip, nodo->puerto, nodo_isNew(nodo) );


		break;
	default:
		printf("mensaje desconocido\n");
		break;
	}
	destroy_message(msg);
}

