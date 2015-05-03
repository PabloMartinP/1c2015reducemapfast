/*
 * fileSystem.h
 *
 *  Created on: 30/4/2015
 *      Author: utnso
 */

#ifndef FILESYSTEM_H_
#define FILESYSTEM_H_

//#include <util.h>

#include "nodo.h"
#include "directorios.h"
#include "archivos.h"

typedef struct {
	t_nodo* nodo;
	int n_bloque;
} t_nodo_bloque;

typedef struct {
	//int cant_bloques;
	t_list* nodos;
	t_list* nodos_no_agregados; //son los nodos conectados pero no agregados al fs
	t_list* directorios;
	t_list* archivos;

} t_fileSystem;

const int BLOQUE_CANT_COPIAS = 3;

void fs_print_dirs(t_fileSystem* fs);
void fs_agregar_nodo(t_fileSystem* fs, int id_nodo);
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
int cant_bloques_necesarios(char* archivo);
char* file_obtener_bloque(char* mapped, int n_bloque);

int cant_registros(char** registros);
t_list* fs_importar_archivo(t_fileSystem* fs, char* archivo);
t_bloque_de_datos* guardar_bloque(t_fileSystem* fs, char* bloque_origen, size_t offset);
void fs_guardar_bloque(t_nodo_bloque* nb, char* bloque, size_t tamanio_real);
t_nodo_bloque** fs_get_tres_nodo_bloque_libres(t_fileSystem* fs);
bool ordenar_por_mayor_cant_bloques_libres(t_nodo* uno, t_nodo* dos);
t_list* obtener_tres_nodos_disponibles(t_fileSystem* fs);
void bloque_marcar_como_usado(t_bloque* bloque);
void fs_eliminar_nodo(t_fileSystem* fs, int id_nodo);
t_nodo* fs_buscar_nodo_por_id(t_fileSystem* fs, int id_nodo);
void fs_print_nodos(t_list* nodos);
void fs_desconectarse(t_fileSystem* fs);
void fs_enviar_mensaje_desconexion(t_list* nodos);
void fs_print_dirs(t_fileSystem* fs);
void fd_leer_dirs(t_list* dirs);
void fs_formatear(t_fileSystem* fs) ;
void fd_leer_archivos(t_list* archivo);
void fs_copiar_archivo_local_al_fs(t_fileSystem* fs, char* archivo, int dir_padre);
/*
 * ****************************************************************************************
 */
void fs_formatear(t_fileSystem* fs) {
//FILE* file1 = fopen(FILE_DIRECTORIO, "w+");
	dir_formatear();
	list_clean(fs->directorios);


	FILE* file2 = fopen(FILE_ARCHIVO, "w+");
	fclose(file2);

	printf("se formateo.................................................\n");
}


void fs_print_dirs(t_fileSystem* fs) {
	printf("LISTA DE DIRECTORIOS\n");

	list_iterate(fs->directorios, (void*) dir_print);

	printf("***********************************\n");
}

/*
 * enviar mensaje a los ndos para que se desconecten
 */
void fs_desconectarse(t_fileSystem* fs) {
	fs_enviar_mensaje_desconexion(fs->nodos);
	fs_enviar_mensaje_desconexion(fs->nodos_no_agregados);
}

void fs_enviar_mensaje_desconexion(t_list* nodos) {
	list_iterate(nodos, (void*) nodo_mensaje_desconexion);
}

t_nodo* fs_buscar_nodo_por_id(t_fileSystem* fs, int id_nodo) {
	bool _buscar_nodo_por_id(t_nodo* nodo) {
		return nodo->identificador == id_nodo;
	}
	return list_find(fs->nodos, (void*) _buscar_nodo_por_id);
}

void fs_agregar_nodo(t_fileSystem* fs, int id_nodo) {
	//busco el id_nodo en la lista de nodos_nuevos
	bool buscar_nodo_nuevo_por_id(t_nodo* nodo) {
		return nodo->identificador == id_nodo;
	}
	//busco el nodo
	t_nodo* nodo;
	if ((nodo = list_find(fs->nodos_no_agregados,(void*) buscar_nodo_nuevo_por_id)) == NULL) {
		printf("El nodo ingresado no existe: %d\n", id_nodo);
		return;
	}

	//loborro de la lista de nodos nuevos
	list_remove_by_condition(fs->nodos_no_agregados,(void*) buscar_nodo_nuevo_por_id);

	//lo paso a la lista de nodos activos del fs
	list_add(fs->nodos, nodo);

}

void fs_eliminar_nodo(t_fileSystem* fs, int id_nodo) {
	t_nodo* nodo = NULL;

	bool buscar_nodo_por_id(t_nodo* nodo) {
		return nodo->identificador == id_nodo;
	}
	nodo = list_find(fs->nodos, (void*) buscar_nodo_por_id);

	//lo saco de la lista
	list_remove_by_condition(fs->nodos, (void*) buscar_nodo_por_id);

	//lo agrego a la lista de nodos no agregados
	list_add(fs->nodos_no_agregados, (void*) nodo);
}

void fs_copiar_archivo_local_al_fs(t_fileSystem* fs, char* nombre, int dir_padre) {
	t_list* bloques_de_dato = NULL;
	//obtengo los bloques y donde esta guardado cada copia
	bloques_de_dato = fs_importar_archivo(fs, nombre);

	//obtengo info del archivo
	t_archivo_info* info = arch_get_info(nombre, dir_padre);

	//creo el archivo
	t_archivo* archivo = malloc(sizeof *archivo);
	archivo->bloques_de_datos = bloques_de_dato;
	archivo->info = info;

	//lo agrego al fs
	arch_agregar(fs->archivos, archivo);
}

/*
 * devuelvo una lista de los t_bloque_de_datos del archivo
 */
t_list* fs_importar_archivo(t_fileSystem* fs, char* archivo) {

	t_list* new = list_create();


	t_bloque_de_datos* bd =NULL;

	size_t size = file_get_size(archivo);
	printf("tamaño total en byes: %zd\n", size);

	int cant_bloq_necesarios = cant_bloques_necesarios(archivo);
	printf("bloques necesarios: %d\n", cant_bloq_necesarios);

	char* mapped = file_get_mapped(archivo);

	int nro_bloque = 0;
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

			bd = guardar_bloque(fs, mapped + offset, bytes_leidos);
			bd->numero = nro_bloque;
			nro_bloque++;
			list_add(new, bd);

			offset = bytes_leidos;
			bytes_leidos = 0;
		}
	}
	//me fijo si quedo algo sin grabar en el bloque
	if (bytes_leidos > 0) {
		bd = guardar_bloque(fs, mapped + offset, bytes_leidos);
		//setteo el nro de bloque
		bd->numero = nro_bloque;
		nro_bloque++;
		//agrego el bloquededatos a la lista
		list_add(new, bd);

	}

	file_mmap_free(mapped, archivo);

	return new;
}


void bloque_marcar_como_usado(t_bloque* bloque) {
	bloque->libre = false;
	bloque->requerido_para_copia = true;
}

t_bloque_de_datos* guardar_bloque(t_fileSystem* fs, char* bloque_origen, size_t bytes_a_copiar) {
	t_bloque_de_datos* new = bloque_de_datos_create();

	int i;
	t_bloque* bloque_usado;
	//genero el bloque a copiar
	char* bloque = malloc(bytes_a_copiar);
	memcpy(bloque, bloque_origen, bytes_a_copiar);

	t_nodo_bloque** nb = NULL;
	//me traigo en un vector los tres t_nodo_bloque donde va a ir la copia del bloque
	nb = fs_get_tres_nodo_bloque_libres(fs);
	for (i = 0; i < BLOQUE_CANT_COPIAS; i++) {
		fs_guardar_bloque(nb[i], bloque, bytes_a_copiar);

		bool buscar_bloque(t_bloque* bloque) {
			return bloque->posicion == nb[i]->n_bloque;
		}

		bloque_usado = list_find(nb[i]->nodo->bloques, (void*) buscar_bloque);
		bloque_marcar_como_usado(bloque_usado);

		printf("nodo %d bloque %d marcado como usado\n", nb[i]->nodo->identificador, nb[i]->n_bloque);

		//agrego el bloquenodo a la lista, al final del for quedaria con las tres copias y faltaria settear el nro_bloque
		list_add(new->nodosbloque, (void*)nb[i]);
		//free_null(nb[i]);
	}
	//free(nb)
	//hacer free de lam matriz

	free_null(bloque);

	return new;
}

void fs_guardar_bloque(t_nodo_bloque* nb, char* bloque, size_t tamanio_real) {
	//me tengo que conectar con el nodo y pasarle el bloque

	printf("iniciando transferencia a Ip:%s:%d bloque %d\n", nb->nodo->ip,
			nb->nodo->puerto, nb->n_bloque);
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
void fs_print_nodos(t_list* nodos) {
	printf("NODOS CONECTADOS AL SISTEMA ('addnodo n' para agregarlos)\n");
	list_iterate(nodos, (void*) print_nodo);
	printf("\n---------------------------------------------------");
}

bool ordenar_por_mayor_cant_bloques_libres(t_nodo* uno, t_nodo* dos) {
	return nodo_cant_bloques_libres(uno) > nodo_cant_bloques_libres(dos);
}

t_list* obtener_tres_nodos_disponibles(t_fileSystem* fs) {
	//ordeno por cantidad de libres

	if (list_size(fs->nodos) >= 3) {
		list_sort(fs->nodos, (void*) ordenar_por_mayor_cant_bloques_libres);
		//genera una lista nueva y la devuelvo
		return list_take(fs->nodos, 3);
	} else {
		//si es menor a tres agarro el primer nodo , verificar si es posible con el ayudante
		//guardaria todo el el mismo nodo y distinto bloque
		t_list* lista = list_create();

		//tomo el primer nodo porque si!, consultar con ayte si si puede pasar que el fs quede con menso de tres nodos y que hacer ....
		list_add(lista, (t_nodo*) list_get(fs->nodos, 0));
		list_add(lista, (t_nodo*) list_get(fs->nodos, 0));
		list_add(lista, (t_nodo*) list_get(fs->nodos, 0));

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
	new = malloc(BLOQUE_CANT_COPIAS * sizeof(t_nodo_bloque*));

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
	fs->nodos_no_agregados = list_create();

	fs->directorios  = list_create();
	fs->archivos = list_create();

	if(file_exists(FILE_DIRECTORIO))
		fd_leer_dirs(fs->directorios);

	if(file_exists(FILE_ARCHIVO) && file_get_size(FILE_ARCHIVO)>0)
		fd_leer_archivos(fs->archivos);
}
void fd_leer_archivos(t_list* archivo){
	char* map = file_get_mapped(FILE_ARCHIVO);
	int cant_reg = file_get_size(FILE_ARCHIVO) / sizeof(t_archivo_info);

	t_archivo_info* info;

	int i;
	for (i = 0; i < cant_reg; i++) {
		info = malloc(sizeof *info);


		memcpy(info, map+(i*sizeof(t_archivo_info)), sizeof(t_archivo_info));
	}


	file_mmap_free(map, FILE_ARCHIVO);
}

void fd_leer_dirs(t_list* dirs) {
	char* map = file_get_mapped(FILE_DIRECTORIO);
	t_directorio* dir;
	dir = malloc(sizeof *dir);
	int i = 0;
	for (i = 0; i < DIR_CANT_MAX; i++) {

		dir = memcpy(dir, map + (i * sizeof(t_directorio)),	sizeof(t_directorio));
		if (dir->index != 0) { //si es igual a 0 esta disponible
			list_add(dirs, (void*)dir);
			dir = malloc(sizeof *dir);
		}
	}
	free_null(dir);
	file_mmap_free(map, FILE_DIRECTORIO);
}

void fs_destroy(t_fileSystem* fs) {
	list_destroy_and_destroy_elements(fs->nodos, (void*) nodo_destroy);
	list_destroy_and_destroy_elements(fs->nodos_no_agregados,
			(void*) nodo_destroy);
	list_destroy(fs->directorios);
	list_destroy(fs->archivos);
}

#endif /* FILESYSTEM_H_ */
