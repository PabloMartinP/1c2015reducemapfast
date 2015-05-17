#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "config_MaRTA.h"

void iniciar_thread_server_MaRTA();
void iniciar_servidor();
void procesar (int socket, t_msg*msg);
int archivo_a_usar();
t_list* nodos_usados();
int hilos_de_mapper();
bool soporta_combiner();
int rutina_con_combiner();
int rutina_sin_combiner();
void enviar_rutina_reduce();


void iniciar_thread_server_MaRTA() {
	/*
	 * ABRO UN THREAD EL SERVER PARA QUE SE CONECTEN NODOS
	 */
	pthread_t th_procesar;
	pthread_attr_t tattr;

	pthread_attr_init(&tattr);
	pthread_attr_setdetachstate(&tattr, PTHREAD_CREATE_DETACHED);
	pthread_create(&th_procesar, &tattr, (void*) iniciar_servidor, NULL);
	pthread_attr_destroy(&tattr);
	//espera a que termine el nodo: bloquea
	//pthread_join(th_nuevosNodos, NULL);
	//pthread_detach(th_nuevosNodos);
}

void iniciar_servidor () {
	printf("iniciando server marta en el puerto %d\n", MaRTA_PUERTO() );
	//queda bloqueado hasta que le llegue algun mensaje
	server_socket_select(MaRTA_PUERTO(),	(void*) procesar);
}

void procesar (int socket, t_msg*msg){
	int res = 0;
	print_msg(msg);

	switch (msg->header.id) {
		case JOB_HOLA:
			destroy_message(msg);

			printf("enviandole respuesta al JOB\n");
			msg = string_message(MARTA_HOLA, "hola job soy marta", 0);
			res = enviar_mensaje(socket, msg);
			destroy_message(msg);
			if(res==-1){
				printf("no se pudo responder el mensaje al job\n");
				break;
			}

			break;
		default:
			break;
	}


}

int rutina_con_combiner(){
	return 0;
}
int rutina_sin_combiner(){
	return 0;
}

void enviar_rutina_reduce(){

}

bool soporta_combiner(){
	return false;
}

int archivo_a_usar(){
	return 0;
}

t_list* nodos_usados(){
	return NULL;
}

int hilos_de_mapper(){
	return 0;
}
