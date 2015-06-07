/*
 * procesoNodo.h
 *
 *  Created on: 23/5/2015
 *      Author: utnso
 */

#ifndef PROCESONODO_H_
#define PROCESONODO_H_

#include <stdio.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <commons/string.h>
#include <commons/temporal.h>
#include <commons/txt.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <commons/log.h>
#include <pthread.h>
#include "config_nodo.h"
#include <nodo.h>
#include <strings.h>
#include <commons/collections/list.h>

#include <util.h>
#include "mapreduce.h"
//#include "socket.h"

char FILE_CONFIG[1024] = "/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoNodo/config.txt";
char FILE_LOG[1024] = "/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoNodo/log.txt";
//char CWD[PATH_MAX_LEN];//para guardar el currentworkingdirectory
/*
 * variables
 */
bool FIN = false;
char* _data = NULL;
t_log* logger = NULL;
/*
 * declaraciones
 */
void* data_get(char* filename);
void data_destroy();
char* getBloque(int32_t numero);
void setBloque(int32_t numero, char* bloque);

char* getFileContent(char* filename);
bool nodo_es_local(char* ip, int puerto);
void inicializar();
void finalizar();
void probar_conexion_fs();
void iniciar_server_thread();
int NODO_CANT_BLOQUES();
char* generar_nombre_reduce_tmp();
void procesar_mensaje(int fd, t_msg* msg);
int aplicar_reduce(t_reduce* reduce, char* script);
char* generar_nombre_map_tmp();
char* generar_nombre_script();
void incicar_server();
void agregar_cwd(char* file);

int ordenar_y_guardar_en_temp(char* file_desordenado, char* destino);
int aplicar_reduce_local_red(t_list* files_reduces, char*script_reduce,
		char* filename_result);

int aplicar_reduce_local(t_list* files, char*script_reduce,
		char* filename_result);
char* convertir_a_temp_path_filename(char* filename);
//char* ejecutar_script_sort(char* filename);
/*
 * graba el temp concatenandole el timenow en el filename para que sea unico
 */
int grabar_en_temp(char* filename, char* data);
bool file_reduce_es_local(t_files_reduce* fr);
bool file_reduce_es_de_red(t_files_reduce* fr);
/*
 * devuelve el archivo creado en el  temp del nodo
 */
int aplicar_map(int n_bloque, char* script_map, char* filename_result);


int TAMANIO_DATA=0;;
int CANT_BLOQUES=0;

#endif /* PROCESONODO_H_ */
