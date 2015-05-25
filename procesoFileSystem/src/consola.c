/*
 * consola.c
 *
 *  Created on: 23/5/2015
 *      Author: utnso
 */

#include "consola.h"




e_comando getComando(char* comando) {

	if (string_equals_ignore_case(comando, "addnodo"))
		return NODO_AGREGAR;
	if (string_equals_ignore_case(comando, "delnodo"))
		return NODO_ELIMINAR;
	if (string_equals_ignore_case(comando, "mkdir"))
		return DIRECTORIO_CREAR;
	if (string_equals_ignore_case(comando, "format"))
		return FORMATEAR;
	if (string_equals_ignore_case(comando, "exit"))
		return SALIR;
	if (string_equals_ignore_case(comando, "lsnodop"))
		return NODO_LISTAR_NO_AGREGADOS;
	if (string_equals_ignore_case(comando, "info"))
		return FS_INFO;
	if (string_equals_ignore_case(comando, "copy"))
		return ARCHIVO_COPIAR_LOCAL_A_MDFS;
	if (string_equals_ignore_case(comando, "copytolocal"))
		return ARCHIVO_COPIAR_MDFS_A_LOCAL;
	if (string_equals_ignore_case(comando, "lsdir"))
		return DIRECTORIO_LISTAR;
	if (string_equals_ignore_case(comando, "fileinfo"))
		return ARCHIVO_INFO;
	if (string_equals_ignore_case(comando, "lsfile"))
		return ARCHIVO_LISTAR;
	if (string_equals_ignore_case(comando, "filevb"))
		return ARCHIVO_VERBLOQUE;
	if (string_equals_ignore_case(comando, "cd"))
		return CAMBIAR_DIRECTORIO;
	if (string_equals_ignore_case(comando, "rmdir"))
		return DIRECTORIO_ELIMINAR;
	if (string_equals_ignore_case(comando, "renamedir"))
		return DIRECTORIO_RENOMBRAR;


	return NADA;
}


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
