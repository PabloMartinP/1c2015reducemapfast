/*
 * nodo.h
 *
 *  Created on: 29/4/2015
 *      Author: utnso
 */

#ifndef NODO_H_
#define NODO_H_


#include <stdbool.h>

typedef struct {
	bool libre;
}t_bloque;

typedef struct {
	int identificador;
	char* ip;
	uint16_t puerto;
	bool isNew;
	t_bloque bloque[50];
} t_nodo;

int id_nodo_nuevo = 0; //se asigna cuando

int get_id_nodo_nuevo();



t_nodo* nodo_new(char* ip, uint16_t port, bool isNew);
char* nodo_isNew(t_nodo* nodo);

#endif /* NODO_H_ */
