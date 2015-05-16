#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

void iniciar_server_MaRTA();
void iniciar_hilo_servidor ();
void procesar (int socket, t_msg*msg);
int archivo_a_usar();
t_list* nodos_usados();
int hilos_de_mapper();
bool soporta_combiner();
int rutina_con_combiner();
int rutina_sin_combiner();
void enviar_rutina_reduce();

char FILE_CONFIG[1024]="/home/utnso/workspace/procesoMaRTA/src/config.txt";
t_config* config;

void iniciar_server_MaRTA() {
	/*
	 * ABRO UN THREAD EL SERVER PARA QUE SE CONECTEN NODOS
	 */
	pthread_t th_procesar;
	pthread_attr_t tattr;

	pthread_attr_init(&tattr);
	pthread_attr_setdetachstate(&tattr, PTHREAD_CREATE_DETACHED);
	pthread_create(&th_procesar, &tattr, (void*) iniciar_hilo_servidor, NULL);
	pthread_attr_destroy(&tattr);
	//espera a que termine el nodo: bloquea
	//pthread_join(th_nuevosNodos, NULL);
	//pthread_detach(th_nuevosNodos);
}

void iniciar_hilo_servidor () {
	server_socket_select(config_get_int_value(config, "PUERTO_LISTEN"),	(void*) procesar);
}

void procesar (int socket, t_msg*msg){
	print_msg(msg);
}


int archivo_a_usar();

t_list* nodos_usados();
