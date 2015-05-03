/*
 * archivos.h
 *
 *  Created on: 2/5/2015
 *      Author: utnso
 */

#ifndef ARCHIVOS_H_
#define ARCHIVOS_H_

#include <commons/txt.h>

typedef struct {
	//t_nodo* nodo;
	int nodo_id;
	int n_bloque;
} t_nodo_bloque;


typedef struct { //estructura que tiene las tres copias del bloque
	int n_bloque; //numero de bloque
	t_list* nodosbloque; //tiene tres estructuras t_nodo_bloque
} t_bloque_de_datos;

typedef struct {
	char nombre[128];
	long int tamanio;
	int directorio;
	bool estado;
	int cant_bloques;
} t_archivo_info;

typedef struct {
	t_archivo_info* info;
	t_list* bloques_de_datos; //aca guardo el nodo y el bloque a donde estan guardados los datos
} t_archivo;

size_t bloque_de_datos_size();
void arch_agregar(t_archivo* archivo);
t_bloque_de_datos* bloque_de_datos_create();
t_archivo_info* arch_get_info(char* nombre, int dir_padre) ;
void arch_formatear();
t_archivo* arch_crear();

/*
 *********************************************************************
 */

/*
 * devuelo el espacio del n_bloque mas tres veces el espacio de un nodo_bloque
 */
size_t bloque_de_datos_size(){
	t_bloque_de_datos* new = malloc(sizeof *new);
	//return ( sizeof(t_nodo_bloque)* BLOQUE_CANT_COPIAS) + sizeof(new->n_bloque);
	return ( sizeof(t_nodo_bloque)* 3) + sizeof(new->n_bloque);
}

t_archivo* arch_crear(){
	t_archivo* new = malloc(sizeof *new);
	new->bloques_de_datos = list_create();

	return new	;
}

void arch_formatear(){
	clean_file(FILE_ARCHIVO);
	clean_file(FILE_ARCHIVO_BLOQUES);//aca es donde guardo la info de los bloques de cada archivo
}
t_archivo_info* arch_get_info(char* nombre, int dir_padre) {
	t_archivo_info* new = malloc(sizeof *new);
	new->estado = true;
	memset(new->nombre, ' ', 128);
	strcpy(new->nombre, nombre);
	new->tamanio = file_get_size(nombre);
	new->directorio = dir_padre;

	return new;

}

void arch_agregar(t_archivo* archivo) {
	FILE* file = txt_open_for_append(FILE_ARCHIVO);
	//FILE* file = fopen(archivo->info->nombre, "wb");

	//setteo el nro de bloques para despues saber cuanto tengo que leer
	archivo->info->cant_bloques = list_size(archivo->bloques_de_datos);

	//grabo la info
	fwrite(archivo->info, sizeof(t_archivo_info), 1, file);
	txt_close_file(file);

	//grabo todo lo siguiente en el archivo que contiene la info de cada bloque
	file = txt_open_for_append(FILE_ARCHIVO_BLOQUES);

	//ahora tengo que grabar los bloques de datos, cada uno con la ubicacion de sus tres copias
	t_bloque_de_datos* bloquedatos;
	t_nodo_bloque* nodobloque;
	int i,j;
	for (i = 0; i < archivo->info->cant_bloques; i++) {

		//me traigo el bloque i de n_bloques del archivo
		bloquedatos = list_get(archivo->bloques_de_datos, i);

		//grabo primero el nro de bloque, luego las tres copias
		fwrite(&bloquedatos->n_bloque, sizeof(bloquedatos->n_bloque), 1, file);

		//grabo las tres copias, la lista siempre tiene tres elementos

		//for (j = 0; j < BLOQUE_CANT_COPIAS; j++) {
		for (j = 0; j < list_size(bloquedatos->nodosbloque); j++) {
			nodobloque = list_get(bloquedatos->nodosbloque, j);

			fwrite(nodobloque, sizeof(t_nodo_bloque), 1, file);
		}
	}

	txt_close_file(file);

}

t_bloque_de_datos* bloque_de_datos_create() {
	t_bloque_de_datos* new = malloc(sizeof *new);
	new->nodosbloque = list_create();

	return new;
}

#endif /* ARCHIVOS_H_ */
