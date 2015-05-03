/*
 * archivos.h
 *
 *  Created on: 2/5/2015
 *      Author: utnso
 */

#ifndef ARCHIVOS_H_
#define ARCHIVOS_H_

#include <commons/txt.h>

typedef struct { //estructura que tiene las tres copias del bloque
	int numero; //numero de bloque
	t_list* nodosbloque; //tiene tres estructuras t_nodo_bloque
} t_bloque_de_datos;

typedef struct {
	char nombre[128];
	long int tamanio;
	int directorio;
	bool estado;
	int n_bloques;
} t_archivo_info;

typedef struct {
	t_archivo_info* info;
	t_list* bloques_de_datos; //aca guardo el nodo y el bloque a donde estan guardados los datos
} t_archivo;

void arch_crear(t_list* archivos, t_archivo* archivo);
t_bloque_de_datos* bloque_de_datos_create();
t_archivo_info* arch_get_info(char* nombre, int dir_padre) ;

/*
 *********************************************************************
 */
t_archivo_info* arch_get_info(char* nombre, int dir_padre) {
	t_archivo_info* new = malloc(sizeof *new);
	new->estado = true;
	memset(new->nombre, ' ', 128);
	strcpy(new->nombre, nombre);
	new->tamanio = file_get_size(nombre);
	new->directorio = dir_padre;

	return new;

}

void arch_agregar(t_list* archivos, t_archivo* archivo) {
	FILE* file = txt_open_for_append(FILE_ARCHIVO);
	//FILE* file = fopen(archivo->info->nombre, "wb");

	//setteo el nro de bloques para despues saber cuanto tengo que leer
	archivo->info->n_bloques = list_size(archivo->bloques_de_datos);

	//grabo la info
	size_t res = fwrite(archivo->info, sizeof(t_archivo_info), 1, file);
	//ahora tengo que grabar los bloques de datos, cada uno con la ubicacion de sus tres copias


	txt_close_file(file);

	list_add(archivos, archivo);
}

t_bloque_de_datos* bloque_de_datos_create() {
	t_bloque_de_datos* new = malloc(sizeof *new);
	new->nodosbloque = list_create();

	return new;
}

#endif /* ARCHIVOS_H_ */
