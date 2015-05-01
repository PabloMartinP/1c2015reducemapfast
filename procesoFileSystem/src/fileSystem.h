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
	//int cant_bloques;
	t_list* nodos;

}t_fileSystem;

void fs_create(t_fileSystem* fs);
void fs_destroy(t_fileSystem* fs);
void fs_addNodo(t_fileSystem* fs, t_nodo* nodo);
void fs_print_info(t_fileSystem* fs);
int fs_cant_bloques(t_fileSystem* fs);
size_t fs_tamanio_bytes(t_fileSystem* fs);
int fs_tamanio_megabytes(t_fileSystem* fs);
int fs_tamanio_libre_megabytes(t_fileSystem* fs);
int fs_tamanio_usado_megabytes(t_fileSystem* fs);
int fs_cant_bloques_libres(t_fileSystem* fs);
int fs_cant_bloques_usados(t_fileSystem* fs);
/*
 * ****************************************************************************************
 */

void fs_print_info(t_fileSystem* fs){
	printf("INFORMACION ACTUAL DEL FS\n");

	printf("Tamanio : %zd bytes\n", fs_tamanio_bytes(fs));
	printf("Tamanio : %d MB\n", fs_tamanio_megabytes(fs));
	printf("Tamanio libre: %d MB\n", fs_tamanio_libre_megabytes(fs));
	printf("Tamanio usado: %d MB\n", fs_tamanio_usado_megabytes(fs));

	printf("Cant Nodos conectados: %d\n", list_size(fs->nodos));

	printf("Cant bloques: %d\n", fs_cant_bloques(fs));
	printf("Cant bloques libres: %d\n", fs_cant_bloques_libres(fs));
	printf("Cant bloques usados : %d\n", fs_cant_bloques_usados(fs));

	printf("*********************************************\n");
}

int fs_cant_bloques_usados(t_fileSystem* fs){
	return fs_cant_bloques(fs) - fs_cant_bloques_libres(fs);
}

int fs_tamanio_usado_megabytes(t_fileSystem* fs){
	return fs_tamanio_megabytes(fs) - fs_tamanio_libre_megabytes(fs);
}

int fs_tamanio_libre_megabytes(t_fileSystem* fs){
	return fs_cant_bloques_libres(fs) * TAMANIO_BLOQUE_MB;
}

int cant_bloques_usados(t_fileSystem* fs){
	return fs_cant_bloques(fs) - fs_cant_bloques_libres(fs);
}

void fs_addNodo(t_fileSystem* fs, t_nodo* nodo){
	list_add(fs->nodos, nodo);

	//actualizo el tamaño del nodo
	//fs->cant_bloques = fs->cant_bloques + CANT_BLOQUES;
}

int fs_cant_bloques_libres(t_fileSystem* fs){
	int cant=0;

	void cant_bloques_libres(t_nodo* nodo){
		cant+= nodo_cant_bloques_libres(nodo);
	}

	list_iterate(fs->nodos, (void*)cant_bloques_libres);

	return cant;
}

int fs_cant_bloques(t_fileSystem* fs){
	size_t size = 0;

	void contar_bloques(t_nodo* nodo){
		size += nodo_cant_bloques(nodo);
	}

	list_iterate(fs->nodos, (void*)contar_bloques);

	return size;
}

size_t fs_tamanio_bytes(t_fileSystem* fs){
	return fs_cant_bloques(fs) * TAMANIO_BLOQUE_B;
}

int fs_tamanio_megabytes(t_fileSystem* fs){
	return (fs_cant_bloques(fs) * TAMANIO_BLOQUE_B) / (MB_EN_B);
}


void fs_create(t_fileSystem* fs){
	fs->nodos = list_create();

	//fs->size_in_bytes = 0;
}

void fs_destroy(t_fileSystem* fs){
	list_destroy(fs->nodos);
	//free(fs);
}

#endif /* FILESYSTEM_H_ */
