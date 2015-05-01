/*
 * fileSystem.h
 *
 *  Created on: 30/4/2015
 *      Author: utnso
 */

#ifndef FILESYSTEM_H_
#define FILESYSTEM_H_

#include <util.h>
#include <commons/collections/list.h>
#include <nodo.h>


typedef struct {
	long int size_in_bytes;
	int size_in_blocks;
	t_list* nodos;

}t_fileSystem;

void fs_create(t_fileSystem* fs);
void fs_destroy(t_fileSystem* fs);
void fs_addNodo(t_fileSystem* fs, t_nodo* nodo);

/*
 * ****************************************************************************************
 */


void fs_addNodo(t_fileSystem* fs, t_nodo* nodo){
	list_add(fs->nodos, nodo);

	//actualizo el tamaño del nodo
	fs->size_in_bytes =fs->size_in_bytes + DATA_SIZE;
	fs->size_in_blocks = fs->size_in_blocks + CANT_BLOQUES;
}
void fs_create(t_fileSystem* fs){
	fs->nodos = list_create();

	fs->size_in_bytes = 0;
}

void fs_destroy(t_fileSystem* fs){
	list_destroy(fs->nodos);
	//free(fs);
}

#endif /* FILESYSTEM_H_ */
