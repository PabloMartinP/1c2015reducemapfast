/*
 * nodo.h
 *
 *  Created on: 29/4/2015
 *      Author: utnso
 */

#ifndef NODO_H_
#define NODO_H_

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include "commons/collections/list.h"
#include "commons/string.h"
#include <stdint.h>

#include "util.h"

typedef struct{
	char ip[15];
	int puerto;
	int numero_bloque;
}t_conexion_nodo_bloque;

#define TAMANIO_BLOQUE_MB 20
#define TAMANIO_BLOQUE_B  (1024 * 1024 * 20) //20mb

typedef struct { //estructura que tiene las tres copias del bloque
	int n_bloque; //numero de bloque
	t_list* nodosbloque; //tiene tres estructuras t_nodo_bloque
} t_bloque_de_datos;

typedef struct {
	uint32_t posicion;
	bool libre;
	bool requerido_para_copia;//cuando pido el bloque para copiar pero todavia no esta confirmado el cambio, todavia sigue libre
}t_bloque;

typedef struct {
	int id;
	char ip[15];
	int puerto;
	int cant_bloques;
}t_nodo_base;
typedef struct {
	t_nodo_base base;
	//int id;
	//char ip[15];
	//uint16_t puerto;
	bool conectado;
	bool esNuevo;
	t_list* bloques;
} t_nodo;

typedef struct{
	int nodo_id;
	int n_bloque;
}t_nodo_id_n_bloque; //se usa para grabar en el archivo de bloques

typedef struct {
	t_nodo* nodo;
	//int nodo_id;
	int n_bloque;
} t_nodo_bloque;

t_nodo* nodo_new(char* ip, int port, bool isNew, int cant_bloques);
void print_nodo(t_nodo* nodo);

bool bloque_esta_usado(t_bloque* bloque);
bool bloque_esta_libre(t_bloque* bloque);

t_bloque* nodo_get_bloque_libre(t_nodo* nodo);
t_bloque* nodo_get_bloque_para_copiar(t_nodo* nodo);

int nodo_cant_bloques_libres(t_nodo* nodo);
int nodo_cant_bloques_usados(t_nodo* nodo);
int nodo_cant_bloques(t_nodo* nodo);
void nodo_marcar_como_libre_total(t_nodo* nodo);

void nodo_destroy(t_nodo* nodo);
void nodo_set_ip(t_nodo* nodo, char* ip);
t_bloque* nodo_buscar_bloque(t_nodo* nodo, int n_bloque);
void nodo_marcar_bloque_como_usado(t_nodo* nodo, int n_bloque);
char* nodo_isNew(t_nodo* nodo);
void nodo_print_info(t_nodo* nodo);
void nodo_mensaje_desconexion(t_nodo* nodo);
bool nodo_base_igual_a(t_nodo_base nb, t_nodo_base otro_nb);

//bool nodo_esta_vivo(t_nodo* nodo);
bool nodo_esta_vivo(char* ip, int puerto);

#endif /* NODO_H_ */
