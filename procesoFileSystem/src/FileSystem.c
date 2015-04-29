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
#include <commons/string.h>
#include <strings.h>
#include <commons/log.h>
#include <commons/config.h>
#include <unistd.h>
#include <commons/collections/list.h>

#include <socket.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <util.h>
#include <pthread.h>

#include "consola.h"


#define FILE_CONFIG "/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoFileSystem/config.txt"
#define FILE_LOG "/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoFileSystem/log.txt"

typedef struct {
	int identificador;
	char* ip;
	uint16_t puerto;
} t_nodo;

t_log* logger;
t_config* config;

t_list* nodos;
t_list* nodos_pendientes; //los nodos que estan disponibles pero estan agregados al fs


void nuevosNodos();
void procesar_mensaje_nodo(int i, t_msg* msg);
void inicializar();
void fin();


int main(void) {

	inicializar();

	iniciar_consola();

	return EXIT_SUCCESS;
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
}

void nuevosNodos() {
	fd_set master, read_fds;
	int fdNuevoNodo, fdmax, newfd;
	int i;

	fdNuevoNodo = server_socket(config_get_int_value(config, "PUERTO_LISTEN"));

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
					char * ip;
					newfd = accept_connection_and_get_ip(fdNuevoNodo, &ip);

					printf("nueva conexion desde IP: %s\n", ip);
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
	print_msg(msg);

	switch (msg->header.id) {
	case NODO_CONECTAR_CON_FS: //primer mensaje del nodo
		destroy_message(msg);
		msg = string_message(FS_NODO_OK, "", 0);
		enviar_mensaje(i, msg);




		break;
	default:
		printf("mensaje desconocido\n");
		break;
	}
}

