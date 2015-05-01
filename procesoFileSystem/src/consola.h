/*
 * consola.h
 *
 *  Created on: 29/4/2015
 *      Author: utnso
 */

#ifndef CONSOLA_H_
#define CONSOLA_H_

const int COMMAND_MAX_SIZE = 256;

const char* FILE_DIRECTORIO = "directorio.txt";
const char* FILE_ARCHIVO = "archivo.txt";


typedef enum {
	NODO_AGREGAR,
	NODO_ELIMINAR,
	FORMATEAR,
	NADA,
	SALIR,
	DIRECTORIO_CREAR,
	DIRECTORIO_RENOMBRAR,
	DIRECTORIO_ELIMINAR,
	DIRECTORIO_MOVER,
	DIRECTORIO_LISTAR,
	ARCHIVO_COPIAR_LOCAL_MDFS,
	COPIAR_RENOMBRAR,
	ARCHIVO_ELIMINAR,
	ARCHIVO_MOVER,
	ARCHIVO_LISTAR,
	ARCHIVO_COPIAR_MDFS_LOCAL,
	NODO_LISTAR_NO_AGREGADOS,
	FS_INFO
} e_comando;

typedef struct {
	int index;
	char nombre[128];
	int padre;
} t_directorio;
typedef struct {
	char nombre[128];
	long int tamanio;
	int directorio;
	bool estado;
//falta la lista de nodos
} t_archivo;


e_comando getComando(char* input_user);

void formatear();
void directorio_crear(char* comando);
void iniciar_consola();


/*
 *
 */

#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <commons/string.h>

#include <util.h>
#include "consola.h"



e_comando getComando(char* comando) {


	if (string_starts_with(comando, "addnodo"))
		return NODO_AGREGAR;
	if (string_starts_with(comando, "eliminarnodo"))
		return NODO_ELIMINAR;
	if (string_starts_with(comando, "mkdir"))
		return DIRECTORIO_CREAR;
	if (string_starts_with(comando, "formatear"))
		return FORMATEAR;
	if (string_starts_with(comando, "salir"))
		return SALIR;
	if (string_starts_with(comando, "lsnodop"))
		return NODO_LISTAR_NO_AGREGADOS;
	if (string_starts_with(comando, "info"))
			return FS_INFO;
	if (string_starts_with(comando, "copiar"))
			return ARCHIVO_COPIAR_LOCAL_MDFS;


	return NADA;
}





void directorio_crear(char* comando) {
	FILE* file = fopen(FILE_DIRECTORIO, "a+");

	t_directorio dir;
	dir.index = 1; //dir_ultimoIndex();
	strcpy(dir.nombre, "un directorio");
	dir.padre = 0;

//fwrite(&dir, sizeof(t_directorio), 1, file);
	fprintf(file, "hayque grabar el directorio");

	fclose(file);

	printf("grabo el dir\n");
}

void formatear() {
	FILE* file1 = fopen(FILE_DIRECTORIO, "w+");
	FILE* file2 = fopen(FILE_ARCHIVO, "w+");
	fclose(file1);
	fclose(file2);
	printf("se formateo\n");
}



#endif /* CONSOLA_H_ */
