/*
 * nodo.c
 *
 *  Created on: 29/4/2015
 *      Author: utnso
 */
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include <commons/collections/list.h>
#include "util.h"

#include "nodo.h"



//bool nodo_esta_vivo(t_nodo* nodo){
bool nodo_esta_vivo(char* ip, int puerto){
	bool on ;
	//obtengo un socket cliente para ver si responde
	//int fd = client_socket(nodo->ip, nodo->puerto);
	int fd = client_socket(ip, puerto);

	//le mando handshake a ver si me responde el HOLA
	t_msg* msg = string_message(NODO_HOLA, "",0);
	enviar_mensaje(fd, msg);
	destroy_message(msg);
	msg = recibir_mensaje(fd);

	//si responde NODO_HOLA esta activo
	on = msg->header.id == NODO_HOLA;

	destroy_message(msg);

	return on;
}

/*
 * envio un mensaje de desconexion
 */
void nodo_mensaje_desconexion(t_nodo* nodo){

	printf("Enviar msg de desconexion al nodo %d, ip:%s:%d\n", nodo->id, nodo->ip, nodo->puerto);
	//me conecto al nodo
	int fd = client_socket(nodo->ip, nodo->puerto);

	t_msg* msg = string_message(NODO_CHAU, "",0);

	enviar_mensaje(fd, msg);
	destroy_message(msg);

}

bool bloque_esta_usado(t_bloque* bloque){
	return !bloque->libre;
}
bool bloque_esta_libre(t_bloque* bloque){
	return bloque->libre;
}

bool bloque_esta_para_copiar(t_bloque* bloque){
	return !bloque->requerido_para_copia;
}

/*
 * devuelvo un bloque libre cualquiera
 */
t_bloque* nodo_get_bloque_libre(t_nodo* nodo){
	t_bloque* b;

	b = list_find(nodo->bloques, (void*)bloque_esta_libre);

	return b;
}


t_bloque* nodo_get_bloque_para_copiar(t_nodo* nodo){
	t_bloque* b;

	b = list_find(nodo->bloques, (void*)bloque_esta_para_copiar);
	if(b == NULL){
		printf("El nodo %d no tiene bloques libres\n", nodo->id);
		return NULL;
	}
	b->requerido_para_copia = true;

	return b;
}


int nodo_cant_bloques_libres(t_nodo* nodo){
	return list_count_satisfying((void*)nodo->bloques, (void*)bloque_esta_libre );
}
int nodo_cant_bloques_usados(t_nodo* nodo){
	return list_count_satisfying((void*)nodo->bloques, (void*)bloque_esta_usado );
}
int nodo_cant_bloques(t_nodo* nodo){
	return list_size((void*)nodo->bloques);
}

void print_nodo(t_nodo* nodo){
	printf("Id: %d, Nuevo: %d, Ip: %s, Puerto: %d\n", nodo->id, nodo->esNuevo, nodo->ip, nodo->puerto);
}


int get_id_nodo_nuevo() {

	ID_NODO_NUEVO++;
	return ID_NODO_NUEVO;
}

char* nodo_isNew(t_nodo* nodo) {
	return (nodo->esNuevo) ? "Nuevo" : "Viejo";
}



void nodo_destroy(t_nodo* nodo) {
	list_destroy(nodo->bloques);
	free_null((void*)&nodo);
	//printf("%s", nodo->ip);
}

void nodo_set_ip(t_nodo* nodo, char* ip){
	memcpy(nodo->ip, ip, strlen(ip));
}

void nodo_marcar_bloque_como_usado(t_nodo* nodo, int n_bloque){
	t_bloque* bloque = NULL;
	bloque = nodo_buscar_bloque(nodo, n_bloque);
	bloque->libre = false;
}

t_bloque* nodo_buscar_bloque(t_nodo* nodo, int n_bloque){
	bool _buscar_bloque(t_bloque* bloque){
		return bloque->posicion == n_bloque;
	}
	return list_find(nodo->bloques, (void*)_buscar_bloque);
}

t_nodo* nodo_new(char* ip, uint16_t port, bool isNew, uint16_t cant_bloques) {
	t_nodo* new = malloc(sizeof *new);

	memset(new->ip, '\0', 15);

	//le asigno un nuevo id solo si es nuevo
	nodo_set_ip(new, ip);

	//new->ip = string_duplicate(ip);
	new->puerto = port;
	new->esNuevo = isNew;

	//reservo el espacio para la cantidad de bloques
	//new->bloques = malloc(cant_bloques*(sizeof(t_bloque)));
	new->bloques = list_create();

	t_bloque* bloque;
	int i;
	for (i = 0; i < cant_bloques; i++) {
		bloque = malloc(sizeof *bloque);
		bloque->posicion = i;
		bloque->libre = true;
		bloque->requerido_para_copia = false;

		list_add(new->bloques, bloque);
	}


	return new;
}
