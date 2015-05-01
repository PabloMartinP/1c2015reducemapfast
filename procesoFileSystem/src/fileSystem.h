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
	int identificador;
	int n_bloque;
}t_nodo_bloque;

typedef struct {
	//int cant_bloques;
	t_list* nodos;

} t_fileSystem;

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
void fs_importar_archivo(t_fileSystem* fs, char* archivo);
int cant_bloques_necesarios(char* archivo);
char* file_obtener_bloque(char* mapped, int n_bloque);

int cant_registros(char** registros);
void fs_importar_archivo(t_fileSystem* fs, char* archivo);
void guardar_bloque(t_fileSystem* fs, char* fuente, size_t offset);
t_nodo_bloque* fs_get_bloque_libre(t_fileSystem* fs);
void fs_guardar_bloque(t_nodo_bloque* nb, char* bloque);
/*
 * ****************************************************************************************
 */

void fs_importar_archivo(t_fileSystem* fs, char* archivo) {

	size_t size = file_get_size(archivo);
	printf("tamaño total en byes: %zd\n", size);

	int cant_bloq_necesarios = cant_bloques_necesarios(archivo);
	printf("bloques necesarios: %d\n", cant_bloq_necesarios);

	char* mapped = file_get_mapped(archivo);

	//spliteo por enter
	char** registros = string_split(mapped, "\n");
	int c_registros = cant_registros(registros);
	printf("cant registros: %d\n", c_registros);

	int i;
	size_t bytes_leidos = 0, offset = 0;

	for (i = 0; i < c_registros; i++) {

		if (bytes_leidos + strlen(registros[i])+1 < TAMANIO_BLOQUE_B) {
			bytes_leidos += strlen(registros[i])+1;
		} else {
			//si supera el tamaño de bloque grabo
			guardar_bloque(fs, mapped+offset, bytes_leidos);
			offset = bytes_leidos;
			bytes_leidos = 0;

		}
	}
	//me fijo si quedo algo sin grabar en el bloque
	if (bytes_leidos > 0) {
		guardar_bloque(fs, mapped+offset, bytes_leidos);
	}

	file_mmap_free(mapped, archivo);
}

void guardar_bloque(t_fileSystem* fs, char* fuente, size_t offset){
	char* bloque = malloc(TAMANIO_BLOQUE_B);
	////grabo 0 en todo el bloque.
	memset(bloque, 0, TAMANIO_BLOQUE_B);
	memcpy(bloque, fuente, offset);


	//busco un bloque libre dentro de un nodo
	t_nodo_bloque* nb = fs_get_bloque_libre(fs);

	//guardo el bloque en el bloque n del nodo x
	fs_guardar_bloque(nb, bloque);


	free_null(bloque);
}

void fs_guardar_bloque(t_nodo_bloque* nb, char* bloque){

}

/*
 * tengo que devolver un bloque libre para guardar los datos de forma tal que este balanceado
 */
t_nodo_bloque* fs_get_bloque_libre(t_fileSystem* fs){
	//por ahora tomo el primer nodo y listo
	t_nodo_bloque* new = malloc(sizeof *new);

	t_nodo* nodo = (t_nodo*)	list_get(fs->nodos, 0);//AGARRO EL DE LA POSICION 0 PORQUE SI
	t_bloque* bloque = nodo_get_bloque_libre(nodo);

	new->identificador = nodo->identificador;
	new->n_bloque = bloque->posicion;

	return new;
}

int cant_registros(char** registros) {
	int i = 0;
	while (registros[i] != NULL) {
		i++;
	}
	return i;
}

/*
 * devuelvo 20mb con el pedazo de bloque leido hasta el \n
 */
char* file_obtener_bloque(char* mapped, int n_bloque) {
	return NULL;
}

int cant_bloques_necesarios(char* archivo) {
	int cant = 0;
	size_t size = file_get_size(archivo);

	cant = size / TAMANIO_BLOQUE_B;

//si no es multiplo del tamaño del bloque sumo 1
	if (size % TAMANIO_BLOQUE_B != 0)
		cant++;

	return cant;
}

void fs_print_info(t_fileSystem* fs) {
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

int fs_cant_bloques_usados(t_fileSystem* fs) {
	return fs_cant_bloques(fs) - fs_cant_bloques_libres(fs);
}

int fs_tamanio_usado_megabytes(t_fileSystem* fs) {
	return fs_tamanio_megabytes(fs) - fs_tamanio_libre_megabytes(fs);
}

int fs_tamanio_libre_megabytes(t_fileSystem* fs) {
	return fs_cant_bloques_libres(fs) * TAMANIO_BLOQUE_MB;
}

int cant_bloques_usados(t_fileSystem* fs) {
	return fs_cant_bloques(fs) - fs_cant_bloques_libres(fs);
}

void fs_addNodo(t_fileSystem* fs, t_nodo* nodo) {
	list_add(fs->nodos, nodo);

//actualizo el tamaño del nodo
//fs->cant_bloques = fs->cant_bloques + CANT_BLOQUES;
}

int fs_cant_bloques_libres(t_fileSystem* fs) {
	int cant = 0;

	void cant_bloques_libres(t_nodo* nodo) {
		cant += nodo_cant_bloques_libres(nodo);
	}

	list_iterate(fs->nodos, (void*) cant_bloques_libres);

	return cant;
}

int fs_cant_bloques(t_fileSystem* fs) {
	size_t size = 0;

	void contar_bloques(t_nodo* nodo) {
		size += nodo_cant_bloques(nodo);
	}

	list_iterate(fs->nodos, (void*) contar_bloques);

	return size;
}

size_t fs_tamanio_bytes(t_fileSystem* fs) {
	return fs_cant_bloques(fs) * TAMANIO_BLOQUE_B;
}

int fs_tamanio_megabytes(t_fileSystem* fs) {
	return (fs_cant_bloques(fs) * TAMANIO_BLOQUE_B) / (MB_EN_B);
}

void fs_create(t_fileSystem* fs) {
	fs->nodos = list_create();

//fs->size_in_bytes = 0;
}

void fs_destroy(t_fileSystem* fs) {
	list_destroy(fs->nodos);
//free(fs);
}

#endif /* FILESYSTEM_H_ */
