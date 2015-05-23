/*
 * directorios.h
 *
 *  Created on: 2/5/2015
 *      Author: utnso
 */

#ifndef DIRECTORIOS_H_
#define DIRECTORIOS_H_

#include <stdbool.h>
#include <stdint.h>
#include <commons/collections/list.h>
#include <util.h>

#define DIR_TAMANIO_MAX_NOMBRE 128
#define DIR_CANT_MAX 1024

//char FILE_DIRECTORIO[1024] = "/mdfs_directorios.bin";
#define FILE_DIRECTORIO "mdfs_directorios.bin"

typedef struct {
	int index;
	char nombre[128];
	int padre;
} t_directorio;




void dir_print(t_directorio* dir);
uint16_t dir_obtenerUltimoIndex() ;
void dir_crear(t_list* dirs, char* nombre, uint16_t padre);
void dir_formatear();
t_directorio* dir_buscar_por_id(t_list* dirs, int id);
/*
 * *********************************************************
 */





#endif /* DIRECTORIOS_H_ */
