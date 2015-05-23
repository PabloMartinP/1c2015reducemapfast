/*
 * fileSystem.h
 *
 *  Created on: 30/4/2015
 *      Author: utnso
 */

#ifndef FILESYSTEM_H_
#define FILESYSTEM_H_

//#include <util.h>

#include <nodo.h>
#include "directorios.h"
#include "archivos.h"
#include <commons/temporal.h>


//char FILE_NODOS[1024] =	"/mdfs_nodos.bin";
#define FILE_NODOS "mdfs_nodos.bin"
#define ID_NODO_INIT 1
//int ID_NODO_NUEVO = 0;

typedef struct {
	int id_nodo_nuevo;
	int cant_nodos_minima;
	bool operativo;
	t_list* nodos;
	t_list* nodos_no_agregados; //son los nodos conectados pero no agregados al fs
	t_list* directorios;
	t_list* archivos;

} t_fileSystem;

t_fileSystem fs;

//const int BLOQUE_CANT_COPIAS = 3;
#define BLOQUE_CANT_COPIAS 3

void fs_print_dirs();
int fs_agregar_nodo(int id_nodo);
int fs_create();
void fs_destroy();
void fs_addNodo(t_nodo* nodo);
void fs_print_info();
int fs_cant_bloques();
size_t fs_tamanio_bytes();
int fs_cant_bloques_libres();
int fs_cant_bloques_usados();
int cant_bloques_necesarios(char* archivo);
char* file_obtener_bloque(char* mapped, int n_bloque);
int nodo_get_new_nodo_id();
size_t fs_tamanio_usado_bytes();
size_t fs_tamanio_libre_bytes();

int cant_registros(char** registros);
t_list* fs_importar_archivo(char* archivo);
t_bloque_de_datos* guardar_bloque(char* bloque_origen,
		size_t offset);
void fs_guardar_bloque(t_nodo_bloque* nb, char* bloque,
		size_t tamanio_real);
t_nodo_bloque** fs_get_tres_nodo_bloque_libres();
bool ordenar_por_mayor_cant_bloques_libres(t_nodo* uno, t_nodo* dos);
t_list* obtener_tres_nodos_disponibles();
void bloque_marcar_como_usado(t_bloque* bloque);
void fs_eliminar_nodo(int id_nodo);
t_nodo* fs_buscar_nodo_por_id(int id_nodo);
bool fs_existe_en_archivo_nodos(t_nodo_base nodo);
void fs_print_nodos_no_agregados();
void fs_desconectarse();
void fs_enviar_mensaje_desconexion(t_list* nodos);
void fs_print_dirs();
void fd_leer_dirs(t_list* dirs);
void fs_formatear();
void fd_leer_archivos(t_list* archivo);
int fs_copiar_archivo_local_al_fs(char* archivo,int dir_padre);
void fs_print_archivo(char* nombre, int dir_id);
t_archivo* fs_buscar_archivo_por_nombre(t_list* archivos, char* nombre,	int dir_id);
bool fs_existe_archivo(char* nombre, int dir_id);
void fs_copiar_mdfs_a_local(char* nombre, int dir_id,
		char* destino);
bool fs_existe_dir(int dir_id);
void fs_print_archivos();
int fs_marcar_nodo_como_desconectado(t_nodo* nodo);
char* bloque_de_datos_traer_data(t_list* nodosBloque);
char* fs_archivo_get_bloque(char* nombre, int dir_id,	int n_bloque);
int fs_archivo_ver_bloque(char* nombre, int dir_id,	int n_bloque);
bool fs_esta_operativo();
bool fs_existe_nodo_por_id(int nodo_id);
void fs_cargar_nodo_base(t_nodo_base* destino, int nodo_id);
t_nodo* fs_buscar_nodo_por_ip_puerto(char* ip, uint16_t puerto);
int fs_get_nodo_id_en_archivo_nodos(char* ip, uint16_t puerto);
void fs_cargar();


/*
 * *****************************************************************************************
 */

#endif /* FILESYSTEM_H_ */

