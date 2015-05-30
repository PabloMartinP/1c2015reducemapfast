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
//char FILE_DIRECTORIO[1024] = "/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoFileSystem/mdfs_directorios.bin";
char FILE_DIRECTORIO[1024];


typedef struct {
	int index;
	char nombre[128];
	int padre;
} t_directorio;




void dir_print(t_directorio* dir);
uint16_t dir_obtenerUltimoIndex() ;
int dir_crear(t_list* dirs, char* nombre, uint16_t padre);
void dir_formatear();
t_directorio* dir_buscar_por_id(t_list* dirs, int id);
t_directorio* dir_buscar_por_nombre(t_list* dirs, char* nombre, int padre);
int dir_renombrar(t_list* dirs, int id, char* nuevo_nombre);
void dir_destroy(t_directorio* dir);
int dir_eliminar_por_id(t_list* list, int id);
int dir_eliminar_por_nombre(char* nombre, int padre);
/*
 * *********************************************************
 */

int dir_eliminar_por_id(t_list* dirs, int id){

	char* map = file_get_mapped(FILE_DIRECTORIO);

	t_directorio* dir = malloc(sizeof*dir);
	//leo la info
	memcpy(dir, map + ((id-1)*sizeof(t_directorio)), sizeof(t_directorio));
	//dir_print(dir);
	dir->index=0;
	memset(dir->nombre, 0, sizeof(dir->nombre));
	dir->padre = -1;
	//grabo
	memcpy(map + ((id-1)*sizeof(t_directorio)), dir, sizeof(t_directorio));

	//limpio
	free(dir);dir = NULL;
	file_mmap_free(map, FILE_DIRECTORIO);

	bool _dir_buscar_por_id(t_directorio* dir){
		return dir->index == id;
	}
	list_remove_and_destroy_by_condition(dirs, (void*)_dir_buscar_por_id, (void*)dir_destroy);


	return 0;
}

void dir_destroy(t_directorio* dir){
	free(dir);
	dir=NULL;
}

int dir_renombrar(t_list* dirs, int id, char* nuevo_nombre){

	char* map = file_get_mapped(FILE_DIRECTORIO);

	t_directorio* dir = malloc(sizeof *dir);
	//leo la info
	memcpy(dir, map + ((id - 1) * sizeof(t_directorio)), sizeof(t_directorio));
	//dir_print(dir);
	//modifico el nombre
	strcpy(dir->nombre, nuevo_nombre);
	//grabo
	memcpy(map + ((id - 1) * sizeof(t_directorio)), dir, sizeof(t_directorio));

	//limpio
	free(dir);
	dir = NULL;
	file_mmap_free(map, FILE_DIRECTORIO);


	dir = dir_buscar_por_id(dirs, id);
	strcpy(dir->nombre, nuevo_nombre);

	return 0;
}

t_directorio* dir_buscar_por_nombre(t_list* dirs, char* nombre, int padre){
	bool _dir_buscar_por_nombre(t_directorio* dir){
		return string_equals_ignore_case(dir->nombre, nombre) && dir->padre == padre;
	}
	return list_find(dirs, (void*)_dir_buscar_por_nombre);
}

t_directorio* dir_buscar_por_id(t_list* dirs, int id){
	bool _dir_buscar_por_id(t_directorio* dir){
		return dir->index == id;
	}
	return list_find(dirs, (void*)_dir_buscar_por_id);
}

/*
 * obtengo un indice libre, si no entran mas devuelve -1;
 */
uint16_t dir_obtenerUltimoIndex() {


	char* map = file_get_mapped(FILE_DIRECTORIO);
	int index_new = -1;

	//leo hasta encontrar un index distinto de -1
	t_directorio* dir;
	dir = malloc(sizeof *dir);
	int i = 0;
	for (i = 0; i < DIR_CANT_MAX; i++) {

		dir = memcpy(dir, map + (i * sizeof(t_directorio)),	sizeof(t_directorio));
		if (dir->index == 0) { //si es igual a 0 esta disponible
			index_new = i + 1; //guardo la posicion para devolverla
			break;
		}
	}
	FREE_NULL(dir);
	file_mmap_free(map, FILE_DIRECTORIO);

	return index_new;
}

void dir_print(t_directorio* dir) {
	printf("Index: %d, Padre: %d, Nombre: %s\n", dir->index, dir->padre, dir->nombre);
}

int dir_crear(t_list* dirs, char* nombre, uint16_t padre) {
	int index = dir_obtenerUltimoIndex();
	if (index == -1) {
		printf("No se pueden crear mas de %d directorios\n", DIR_CANT_MAX);
		return -1;
	}

	t_directorio* dir = malloc(sizeof *dir);
	dir->index = index;
	strcpy(dir->nombre, nombre);
	dir->padre = padre;

	char* map = file_get_mapped(FILE_DIRECTORIO);
	//en el index tengo el nro de fila, empieza en 1
	memcpy(map + ((dir->index-1)*sizeof(t_directorio)), dir, sizeof(t_directorio));

	//lo agrego a la lista
	list_add(dirs, (void*)dir);

	printf("se creo el directorio %s con padre %d e indice %d\n", dir->nombre,dir->padre, dir->index);

	return 0;
}

void dir_formatear() {
//creo el archivo con el tamaño maximo
	create_file(FILE_DIRECTORIO, DIR_CANT_MAX * (sizeof(t_directorio)));

	char* map = file_get_mapped(FILE_DIRECTORIO);
	t_directorio* dir = NULL;
	dir = malloc(sizeof(t_directorio));
	dir->index = 0;
	memset(dir->nombre, '0', DIR_TAMANIO_MAX_NOMBRE); //al nombre le pongo tod0 espacio
	dir->padre = -1;

	int i;
	for (i = 0; i < DIR_CANT_MAX; i++) {
		memcpy(map + i * sizeof(t_directorio), dir, sizeof(t_directorio));
	}

	FREE_NULL(dir);

	file_mmap_free(map, FILE_DIRECTORIO);

}




#endif /* DIRECTORIOS_H_ */
