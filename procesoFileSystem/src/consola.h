/*
 * consola.h
 *
 *  Created on: 29/4/2015
 *      Author: utnso
 */

#ifndef CONSOLA_H_
#define CONSOLA_H_

#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <commons/string.h>

#include <util.h>
#include "directorios.h"
const int COMMAND_MAX_SIZE = 256;


char* FILE_ARCHIVO = "archivos.txt";

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
	char nombre[128];
	long int tamanio;
	int directorio;
	bool estado;
//falta la lista de nodos
} t_archivo;

e_comando getComando(char* input_user);

void fs_formatear();
void iniciar_consola();
/*
 *
 */

e_comando getComando(char* comando) {

	if (string_starts_with(comando, "addnodo"))
		return NODO_AGREGAR;
	if (string_starts_with(comando, "delnodo"))
		return NODO_ELIMINAR;
	if (string_starts_with(comando, "mkdir"))
		return DIRECTORIO_CREAR;
	if (string_starts_with(comando, "format"))
		return FORMATEAR;
	if (string_starts_with(comando, "salir"))
		return SALIR;
	if (string_starts_with(comando, "lsnodop"))
		return NODO_LISTAR_NO_AGREGADOS;
	if (string_starts_with(comando, "info"))
		return FS_INFO;
	if (string_starts_with(comando, "copy"))
		return ARCHIVO_COPIAR_LOCAL_MDFS;
	if (string_starts_with(comando, "lsdir"))
		return DIRECTORIO_LISTAR;

	return NADA;
}


#endif /* CONSOLA_H_ */
