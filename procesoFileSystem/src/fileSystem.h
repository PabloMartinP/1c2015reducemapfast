/*
 * fileSystem.h
 *
 *  Created on: 30/4/2015
 *      Author: utnso
 */

#ifndef FILESYSTEM_H_
#define FILESYSTEM_H_

#include "util.h"
#include <commons/collections/list.h>
#include "nodo.h"

typedef struct {
	t_nodo* nodo;
	int n_bloque;
} t_nodo_bloque;

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
void guardar_bloque(t_fileSystem* fs, char* bloque_origen, size_t offset);
void fs_guardar_bloque(t_nodo_bloque* nb, char* bloque, size_t tamanio_real);
t_nodo_bloque** fs_get_tres_nodo_bloque_libres(t_fileSystem* fs);
bool ordenar_por_mayor_cant_bloques_libres(t_nodo* uno, t_nodo* dos);
t_list* obtener_tres_nodos_disponibles(t_fileSystem* fs);
void bloque_marcar_como_usado(t_bloque* bloque);
/*
 * ****************************************************************************************
 */
const int NODO_CANT_COPIAS = 3;
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

		if (bytes_leidos + strlen(registros[i]) + 1 < TAMANIO_BLOQUE_B) {
			bytes_leidos += strlen(registros[i]) + 1;
		} else {
			//si supera el tamaño de bloque grabo
			guardar_bloque(fs, mapped + offset, bytes_leidos);
			offset = bytes_leidos;
			bytes_leidos = 0;
		}
	}
	//me fijo si quedo algo sin grabar en el bloque
	if (bytes_leidos > 0) {
		guardar_bloque(fs, mapped + offset, bytes_leidos);
	}

	file_mmap_free(mapped, archivo);
}
void bloque_marcar_como_usado(t_bloque* bloque){
	bloque->libre = false;
	bloque->requerido_para_copia = true;
}
void guardar_bloque(t_fileSystem* fs, char* bloque_origen,size_t bytes_a_copiar) {

	int i;
	t_bloque* bloque_usado;
	//genero el bloque a copiar
	char* bloque = malloc(bytes_a_copiar);
	memcpy(bloque, bloque_origen, bytes_a_copiar);

	t_nodo_bloque** nb = NULL;
	//me traigo en un vector los tres t_nodo_bloque donde va a ir la copia del bloque
	nb = fs_get_tres_nodo_bloque_libres(fs);
	for (i = 0; i < NODO_CANT_COPIAS; i++) {
		fs_guardar_bloque(nb[i], bloque, bytes_a_copiar);

		bool buscar_bloque(t_bloque* bloque) {
			return bloque->posicion == nb[i]->n_bloque;
		}

		bloque_usado = list_find(nb[i]->nodo->bloques, (void*) buscar_bloque);
		bloque_marcar_como_usado(bloque_usado);

		printf("nodo %d bloque %d marcado como usado\n",nb[i]->nodo->identificador, nb[i]->n_bloque);
	}
	//free(nb)
	//hacer free de lam matriz

	free_null(bloque);
}

void fs_guardar_bloque(t_nodo_bloque* nb, char* bloque, size_t tamanio_real) {
	//me tengo que conectar con el nodo y pasarle el bloque

	printf("iniciando transferencia a Ip:%s:%d bloque %d\n", nb->nodo->ip, nb->nodo->puerto, nb->n_bloque);
	int fd = client_socket(nb->nodo->ip, nb->nodo->puerto);

	t_msg* msg;
	msg = string_message(FS_HOLA, "", 0);
	enviar_mensaje(fd, msg);
	destroy_message(msg);
	msg = recibir_mensaje(fd);
	if (msg->header.id == NODO_HOLA) {
		destroy_message(msg);
		//le digo que grabe el blque en el nodo n
		msg = string_message(FS_GRABAR_BLOQUE, bloque, 2, nb->n_bloque,
				tamanio_real);

		enviar_mensaje(fd, msg);
	}

	destroy_message(msg);

	close(fd);

	printf("transferencia realizada OK\n");
}

bool ordenar_por_mayor_cant_bloques_libres(t_nodo* uno, t_nodo* dos) {
	return nodo_cant_bloques_libres(uno) > nodo_cant_bloques_libres(dos);
}


t_list* obtener_tres_nodos_disponibles(t_fileSystem* fs){
	//ordeno por cantidad de libres


	if(list_size(fs->nodos)>=3){
		list_sort(fs->nodos, (void*) ordenar_por_mayor_cant_bloques_libres);
		//genera una lista nueva y la devuelvo
		return list_take(fs->nodos, 3);
	}
	else
	{
		//si es menor a tres agarro el primer nodo , verificar si es posible con el ayudante
		//guardaria todo el el mismo nodo y distinto bloque
		t_list* lista = list_create();

		//tomo el primer nodo porque si!, consultar con ayte si si puede pasar que el fs quede con menso de tres nodos y que hacer ....
		list_add(lista, (t_nodo*)list_get(fs->nodos, 0));
		list_add(lista, (t_nodo*)list_get(fs->nodos, 0));
		list_add(lista, (t_nodo*)list_get(fs->nodos, 0));

		return lista;
	}

	return fs->nodos;
}
/*
 * devuelvo un vector de tres posiciones con la info de donde va cada copia del bloque
 */
t_nodo_bloque** fs_get_tres_nodo_bloque_libres(t_fileSystem* fs) {
	t_nodo_bloque** new = NULL;

	t_list* nodos_destino = obtener_tres_nodos_disponibles(fs);

	/*
	 * todo lo que sigue es crear el vector con los tres t_nodo_bloque
	 */
	//inicializo la matriz de 3 elementos tipo t_nodo_bloque
	new = malloc(NODO_CANT_COPIAS * sizeof(t_nodo_bloque*));

	t_bloque* bloque;
	t_nodo* nodo = NULL;
	//tomo los tres primeros
	int i = 0;
	for (i = 0; i < 3; i++) {
		//reservo espacio para el nodo_bloque
		new[i] = malloc(sizeof(t_nodo_bloque));

		nodo = NULL;
		//tomo el de la posicion i porque la lsita ya esta ordenada por mayor cant de bloques libres
		nodo = (t_nodo*) list_get(nodos_destino, i);
		bloque = NULL;

		//devuelvo un bloque y lo marco como requerido para copiar para que no me traiga
		bloque = nodo_get_bloque_para_copiar(nodo);

		new[i]->nodo = nodo;
		new[i]->n_bloque = bloque->posicion;

	}
	list_destroy(nodos_destino);

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
}

#endif /* FILESYSTEM_H_ */
