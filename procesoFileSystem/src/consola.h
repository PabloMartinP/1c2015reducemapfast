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


char* FILE_ARCHIVO = "/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoFileSystem/archivos.txt";
char* FILE_ARCHIVO_BLOQUES = "/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoFileSystem/archivos-bloques.txt";

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


e_comando getComando(char* input_user);


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
