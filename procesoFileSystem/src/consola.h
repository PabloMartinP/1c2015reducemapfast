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

char* FILE_ARCHIVO =
		"/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoFileSystem/archivos.txt";
char* FILE_ARCHIVO_BLOQUES =
		"/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoFileSystem/archivos-bloques.txt";

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
	ARCHIVO_INFO,
	ARCHIVO_MOVER,
	ARCHIVO_LISTAR,
	ARCHIVO_COPIAR_MDFS_LOCAL,
	NODO_LISTAR_NO_AGREGADOS,
	FS_INFO
} e_comando;

e_comando getComando(char* input_user);

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
	if (string_starts_with(comando, "exit"))
		return SALIR;
	if (string_starts_with(comando, "lsnodop"))
		return NODO_LISTAR_NO_AGREGADOS;
	if (string_starts_with(comando, "info"))
		return FS_INFO;
	if (string_starts_with(comando, "copy"))
		return ARCHIVO_COPIAR_LOCAL_MDFS;
	if (string_starts_with(comando, "lsdir"))
		return DIRECTORIO_LISTAR;
	if (string_starts_with(comando, "fileinfo"))
		return ARCHIVO_INFO;

	return NADA;
}
void leer_comando_consola(char* comando);
//char* consola_leer_param_char(char* param);
char** separar_por_espacios(char* string) ;
/*
 * esta funcion magica saca el \n de una cadena, el fgets me devuele \n y tnego que borrarlo
 */
void strip(char *s);

void strip(char *s) {
	char *p2 = s;
	while (*s != '\0') {
		if (*s != '\t' && *s != '\n') {
			*p2++ = *s++;
		} else {
			++s;
		}
	}
	*p2 = '\0';
}



char** separar_por_espacios(char* string) {
	char** res = string_split(string, " ");
	int i = 0;
	while (res[i] != NULL) {
		strip(res[i]); //saco el \n si es que lo tiene
		i++;
	}
	return res;
}

void leer_comando_consola(char* comando) {
	fgets(comando, COMMAND_MAX_SIZE, stdin);
	//comando[strlen(comando)] = ' '	;

	//memset(comando, ' ', COMMAND_MAX_SIZE-strlen(comando));

}

#endif /* CONSOLA_H_ */
