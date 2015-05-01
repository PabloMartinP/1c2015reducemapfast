/*
 * nodo.h
 *
 *  Created on: 29/4/2015
 *      Author: utnso
 */

#ifndef NODO_H_
#define NODO_H_


#include <stdbool.h>
#include <commons/collections/list.h>

typedef struct {
	uint32_t posicion;
	bool libre;
}t_bloque;

typedef struct {
	int identificador;
	char* ip;
	uint16_t puerto;
	bool esNuevo;
	t_list* bloques;
} t_nodo;


//const int DATA_SIZE = 1024 * 1024 * 50; //50MB
int TAMANIO_BLOQUE_MB = 20;
size_t TAMANIO_BLOQUE_B = 1024 * 1024 * 20; //20mb

//const int CANT_BLOQUES = 50;

int ID_NODO_NUEVO = 0; //se asigna cuando se crea un nodo nuevo
t_nodo* nodo_new(char* ip, uint16_t port, bool isNew, uint16_t cant_bloques);
void print_nodo(t_nodo* nodo);
void print_nodos(t_list* nodos) ;
int get_id_nodo_nuevo();

bool bloque_esta_usado(t_bloque* bloque);
bool bloque_esta_libre(t_bloque* bloque);

t_bloque* nodo_get_bloque_libre(t_nodo* nodo);

int nodo_cant_bloques_libres(t_nodo* nodo);
int nodo_cant_bloques_usados(t_nodo* nodo);
int nodo_cant_bloques(t_nodo* nodo);

void nodo_destroy(t_nodo* nodo);

char* nodo_isNew(t_nodo* nodo);

#endif /* NODO_H_ */
