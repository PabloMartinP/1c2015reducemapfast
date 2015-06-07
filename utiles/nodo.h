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
#define BLOQUE_CANT_COPIAS 3

#define TAMANIO_BLOQUE_MB 20
#define TAMANIO_BLOQUE_B  (1024 * 1024 * 20) //20mb


typedef struct { //estructura que tiene las tres copias del bloque
	int parte_numero; //numero de bloque
	t_list* nodosbloque; //tiene tres estructuras t_archivo_nodo_bloque
} t_archivo_bloque_con_copias;


typedef struct {
	uint32_t posicion;
	bool libre;
	bool requerido_para_copia;//cuando pido el bloque para copiar pero todavia no esta confirmado el cambio, todavia sigue libre
}t_bloque;

typedef struct {
	t_nodo_base* base;
	int cant_bloques;
	bool conectado;
	bool esNuevo;
	t_list* bloques;//lista de t_bloque
} t_nodo;




t_nodo* nodo_new(char* ip, int port, bool isNew, int cant_bloques, int id);
void print_nodo(t_nodo* nodo);

t_archivo_bloque_con_copias* bloque_de_datos_crear();


bool bloque_esta_usado(t_bloque* bloque);
bool bloque_esta_libre(t_bloque* bloque);

void archivo_nodo_bloque_destroy_free_base(t_archivo_nodo_bloque* anb);
void archivo_nodo_bloque_destroy_no_free_base(t_archivo_nodo_bloque* anb);

void bloque_de_datos_destroy_free_base(t_archivo_bloque_con_copias* bloque_de_datos);
void bloque_de_datos_destroy_no_free_base(t_archivo_bloque_con_copias* bloque_de_datos);

//void bloque_de_datos_destroy(t_archivo_bloque_con_copias* bloque_de_datos);
//void archivo_nodo_bloque_destroy(t_archivo_nodo_bloque* anb);
t_bloque* nodo_get_bloque_libre(t_nodo* nodo);
t_bloque* nodo_get_bloque_para_copiar(t_nodo* nodo);


void nodo_archivo_destroy(t_nodo_archivo* na);
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
//bool nodo_base_igual_a(t_nodo_base nb, t_nodo_base otro_nb);

//bool nodo_esta_vivo(t_nodo* nodo);
bool nodo_esta_vivo(char* ip, int puerto);


#endif /* NODO_H_ */
