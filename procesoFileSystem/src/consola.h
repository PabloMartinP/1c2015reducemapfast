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
#include "commons/string.h"
//#include <util.h>
//#include "directorios.h"

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
	ARCHIVO_COPIAR_LOCAL_A_MDFS,
	ARCHIVO_COPIAR_MDFS_A_LOCAL,
	COPIAR_RENOMBRAR,
	ARCHIVO_ELIMINAR,
	ARCHIVO_INFO,
	ARCHIVO_MOVER,
	ARCHIVO_LISTAR,
	ARCHIVO_VERBLOQUE,
	ARCHIVO_COPIAR_MDFS_LOCAL,
	NODO_LISTAR_NO_AGREGADOS,
	FS_INFO,
	CAMBIAR_DIRECTORIO,  //cambiar directorio

} e_comando;

#define COMMAND_MAX_SIZE  256

e_comando getComando(char* input_user);
void leer_comando_consola(char* comando);
//char* consola_leer_param_char(char* param);
char** separar_por_espacios(char* string) ;
/*
 * esta funcion magica saca el \n de una cadena, el fgets me devuele \n y tnego que borrarlo
 */
void strip(char *s);


#endif /* CONSOLA_H_ */
