/*
 * nodo.c
 *
 *  Created on: 29/4/2015
 *      Author: utnso
 */
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include "nodo.h"

int get_id_nodo_nuevo(){
	id_nodo_nuevo++;
	return id_nodo_nuevo;
}

char* nodo_isNew(t_nodo* nodo){
	return (nodo->isNew) ? "Nuevo": "Viejo";
}

void nodo_destroy(t_nodo* nodo) {
	free_null(nodo->ip);
}


t_nodo* nodo_new(char* ip, uint16_t port, bool isNew){
	t_nodo* new = malloc(sizeof *new);

	new->ip = string_duplicate(ip);
	new->puerto = port;
	new->isNew = isNew;

	if(isNew){
		int i;
		for(i = 0;i<50;i++){
			new->bloque[i].libre= true;
		}
		new->bloques_libres = CANT_BLOQUES;
	}



	return new;
}
