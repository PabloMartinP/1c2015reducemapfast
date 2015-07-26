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

#include <commons/log.h>
#include "filesystem_config.h"

//char FILE_NODOS[1024] =	"/mdfs_nodos.bin";
//char FILE_NODOS[1024] =	"/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoFileSystem/mdfs_nodos.bin";
//char FILE_NODOS[1024];

//#define ID_NODO_INIT 1
pthread_mutex_t mutex;
pthread_mutex_t mutex_log;
t_log* logger;

typedef struct {
	int cant_nodos_minima;
	t_list* nodos;
	t_list* nodos_no_agregados; //son los nodos conectados pero no agregados al fs
	t_list* directorios;
	t_list* archivos;

} t_fileSystem;

t_fileSystem fs;

//const int BLOQUE_CANT_COPIAS = 3;


void fs_print_dirs();
int fs_agregar_nodo(int id_nodo);
char* fs_dir_get_path(int dir_index);
int fs_dir_get_index(char* path, int padre);
int fs_dir_get_padre();
int fs_create();
void fs_destroy();
void fs_addNodo(t_nodo* nodo);
void fs_print_info();
int fs_cant_bloques();
size_t fs_tamanio_bytes();
int fs_cant_bloques_libres();
int fs_cant_bloques_usados();
int cant_partes_necesarias(char* archivo);
char* file_obtener_bloque(char* mapped, int numero_bloque);
int nodo_get_new_nodo_id();
size_t fs_tamanio_usado_bytes();
size_t fs_tamanio_libre_bytes();

int cant_registros(char** registros);
t_list* fs_importar_archivo(char* archivo);
t_nodo_base* obtener_nodo_mas_cargado_y_distinto_a_nodo(t_nodo_base* otro_nb, int pos_inicial);
t_list* distribuir_copias(int partes);
int fs_arch_eliminar(char* nombre);
t_archivo_bloque_con_copias* guardar_bloque(char* bloque_origen,	size_t offset);
int fs_guardar_bloque(t_archivo_nodo_bloque* nb, char* bloque,	size_t tamanio_real);
t_archivo_nodo_bloque** fs_get_tres_nodo_bloque_libres();
bool ordenar_por_mayor_cant_bloques_libres(t_nodo* uno, t_nodo* dos);
bool ordenar_por_menor_cant_bloques_libres(t_nodo* uno, t_nodo* dos) ;
t_nodo_base* obtener_nodo_con_mayor_cant_bloques_libres();
t_list* cargar_copia_uno(int partes);
t_nodo_base* obtener_nodo_mas_cargado_y_distinto_a_dos_nodos(t_nodo_base* un_nb, t_nodo_base* otro_nb, int pos_inicial);
t_list* obtener_tres_nodos_disponibles();
void bloque_marcar_como_usado(t_bloque* bloque);
int guardar_parte(char* bloque_origen,size_t bytes_a_copiar, t_archivo_bloque_con_copias* abcc) ;
void fs_eliminar_nodo(int id_nodo);
t_nodo* fs_buscar_nodo_por_id(int id_nodo);
bool fs_existe_en_archivo_nodos(t_nodo_base nodo);
void fs_print_nodos_no_agregados();
void fs_print_no_operativo();
void fs_desconectarse();
void fs_enviar_mensaje_desconexion(t_list* nodos);
void fs_print_dirs();
void fd_leer_dirs(t_list* dirs);
void fs_formatear();

int fs_copiar_archivo_local_al_fs(char* archivo,int dir_padre);
void fs_print_archivo(char* nombre, int dir_id);
t_archivo* fs_buscar_archivo_por_nombre(t_list* archivos, char* nombre,	int dir_id);
int fs_buscar_directorio_id_por_nombre(char* nombre, int padre);
bool fs_existe_archivo(char* nombre, int dir_id);
bool fs_dir_esta_vacio(int id);
int fs_copiar_mdfs_a_local(char* nombre, int dir_id,char* destino);
bool fs_existe_dir(char* nombre,int dir_id);
int fs_dir_renombrar(int id, char* nuevo_nombre);
int fs_dir_eliminar_por_id(int id);
bool fs_existe_dir_por_id(int dir_id);
void fs_print_archivos();
int fs_marcar_nodo_como_desconectado(t_nodo* nodo);
char* bloque_de_datos_traer_data(t_list* nodosBloque);
char* fs_archivo_get_bloque(char* nombre, int dir_id,	int numero_bloque);
int fs_archivo_ver_bloque(char* nombre, int dir_id,	int numero_bloque, char* destino);
bool fs_esta_operativo();
bool fs_existe_nodo_por_id(int nodo_id);
void fs_cargar_nodo_base(t_nodo_base* destino, int nodo_id);
t_nodo* fs_buscar_nodo_por_ip_puerto(char* ip, uint16_t puerto);
int fs_get_nodo_id_en_archivo_nodos(char* ip, uint16_t puerto);
void fs_cargar();


/*
 * *****************************************************************************************
 */



bool fs_esta_operativo() {


	/*
	bool falta_algun_nodo = false;
	int i = 0;
	while (fs.nodos_necesarios[i] != '\0') {
		t_nodo* nodo = NULL;
		nodo = fs_buscar_nodo_por_id(atoi(fs.nodos_necesarios[i]));
		if (nodo !=NULL) {
			if( !nodo->conectado){
				printf("Falta agregar el nodo %d\n", atoi(fs.nodos_necesarios[i]));
				falta_algun_nodo = true;
			}

		}
		else{
			printf("Falta agregar el nodo %d\n", atoi(fs.nodos_necesarios[i]));
			falta_algun_nodo = true;
		}

		i++;
	}

	fs.operativo = !falta_algun_nodo;
	*/
	//fs.operativo = list_size(fs.nodos)>=fs.cant_nodos_minima;

	return list_size(fs.nodos)>=fs.cant_nodos_minima;
}

int fs_archivo_ver_bloque(char* nombre, int dir_id, int numero_bloque, char* destino) {
	char* datos_bloque = NULL;

	if ((datos_bloque = fs_archivo_get_bloque(nombre, dir_id, numero_bloque))	!= NULL) {
		printf("Inicio bloque %d del archivo %s\n", numero_bloque, nombre);
		printf("%s", datos_bloque);
		printf("\n***********************************************");
		printf("Fin bloque %d del archivo %s\n", numero_bloque, nombre);

		return 0;
	}
	else{
		return -1;
	}

}

int nodo_guardar_bloque_en_archivo(t_archivo_nodo_bloque* nb, char* destino){
	//me conecto con el nodo
	int fd = client_socket(nb->base->red.ip, nb->base->red.puerto);
	if(fd<0){
		return -1;
	}
	//le pido el bloque numero_bloque
	t_msg* msg = argv_message(NODO_GET_BLOQUE, 1, nb->numero_bloque);
	enviar_mensaje(fd, msg);
	destroy_message(msg);

	msg = recibir_mensaje(fd);
	if(msg==NULL){
		//close(fd);
		return -1;
	}
	//copy los datos del stream a una copia para devolver
	FILE* file = txt_open_for_append(destino);
	if(file==NULL){
		destroy_message(msg);
		return -1;
	}
	int rs;
	rs = fwrite(msg->stream, msg->header.length, 1, file);
	fclose(file);
	destroy_message(msg);
	//enviar_mensaje_nodo_close(fd);
	if(rs==1){//1 porque devuelve el count
		return 0;
	}else{
		return -1;
	}
}

char* fs_archivo_guardar_bloque(char* nombre, int dir_id, int numero_bloque, char* destino) {
	int i;
	//busco el archivo
	t_archivo* archivo = NULL;
	archivo = fs_buscar_archivo_por_nombre(fs.archivos, nombre, dir_id);

	//busco el bloque
	t_archivo_bloque_con_copias* bloque = NULL;
	bloque = arch_buscar_parte(archivo, numero_bloque);

	//tengo que verificar si alguno de los nodos que tiene la copia esta disponible
	//hardcodeo ip y puerto
	//char* ip = "127.0.0.1";
	//int puerto = 6001;

	t_archivo_nodo_bloque* nb = NULL;

	char* datos_bloque = NULL;
	int rs =-1;
	for (i = 0; i < list_size(bloque->nodosbloque); i++) {
		nb = NULL;
		nb = list_get(bloque->nodosbloque, i);

		t_nodo* nodo = fs_buscar_nodo_por_id(nb->base->id);

		if (nodo != NULL) {

			//if (nodo_esta_vivo(nb->base->red.ip, nb->base->red.puerto)) {
			if (nodo_esta_activo(nb->base)) {
				//si esta vivo inicio la transferencia del bloque
				nb = NULL;
				nb = list_get(bloque->nodosbloque, i);

				rs = nodo_guardar_bloque_en_archivo(nb, destino);
				if(rs==0){
					printf("Archivo %s, bloque: %d, guardado en %s\n", nombre, numero_bloque, destino);
					break;
				}
				/*
				//me conecto con el nodo
				int fd = client_socket(nb->base->red.ip, nb->base->red.puerto);
				//le pido el bloque numero_bloque
				t_msg* msg = argv_message(NODO_GET_BLOQUE, 1,	nb->numero_bloque);
				enviar_mensaje(fd, msg);
				destroy_message(msg);
				msg = recibir_mensaje(fd);
				//print_msg(msg);

				//copy los datos del stream a una copia para devolver
				datos_bloque = malloc(msg->header.length + 1);
				memccpy(datos_bloque, msg->stream, 1, msg->header.length + 1);

				destroy_message(msg);
				enviar_mensaje_nodo_close(fd);
				//close(fd);
				*/


				//break;
			} else {		//el nodo se desconecto
				fs_marcar_nodo_como_desconectado(nodo);
			}
		}else{
			printf("El nodo %d no esta conectado %s:%d\n", nb->base->id, nb->base->red.ip, nb->base->red.puerto);
		}

	}


	//si llego hasta aca significa que las tres copias de bloques que tiene estan desconectadas
	//por lo tanto marco al archivo como no-disponible
	archivo->info->disponible = false;

	return NULL;
}

/*
 * devuelve el bloque, hacer free
 */
char* fs_archivo_get_bloque(char* nombre, int dir_id, int numero_bloque) {
	int i;
	//busco el archivo
	t_archivo* archivo = NULL;
	archivo = fs_buscar_archivo_por_nombre(fs.archivos, nombre, dir_id);

	//busco el bloque
	t_archivo_bloque_con_copias* bloque = NULL;
	bloque = arch_buscar_parte(archivo, numero_bloque);

	//tengo que verificar si alguno de los nodos que tiene la copia esta disponible
	//hardcodeo ip y puerto
	//char* ip = "127.0.0.1";
	//int puerto = 6001;

	t_archivo_nodo_bloque* nb = NULL;

	char* datos_bloque = NULL;

	for (i = 0; i < list_size(bloque->nodosbloque); i++) {
		nb = NULL;
		nb = list_get(bloque->nodosbloque, i);

		t_nodo* nodo = fs_buscar_nodo_por_id(nb->base->id);

		if (nodo != NULL) {

			//if (nodo_esta_vivo(nb->base->red.ip, nb->base->red.puerto)) {
			if (nodo_esta_activo(nb->base)) {
				//si esta vivo inicio la transferencia del bloque
				nb = NULL;
				nb = list_get(bloque->nodosbloque, i);

				//nodo_guardar_bloque_en_archivo(nb, destino);


				//me conecto con el nodo
				int fd = client_socket(nb->base->red.ip, nb->base->red.puerto);
				//le pido el bloque numero_bloque
				t_msg* msg = argv_message(NODO_GET_BLOQUE, 1,	nb->numero_bloque);
				enviar_mensaje(fd, msg);
				destroy_message(msg);
				msg = recibir_mensaje(fd);
				//print_msg(msg);

				//copy los datos del stream a una copia para devolver
				datos_bloque = malloc(msg->header.length + 1);
				memccpy(datos_bloque, msg->stream, 1, msg->header.length + 1);

				destroy_message(msg);
				enviar_mensaje_nodo_close(fd);
				//close(fd);


				return datos_bloque;
				//break;
			} else {		//el nodo se desconecto
				fs_marcar_nodo_como_desconectado(nodo);
			}
		}else{
			printf("El nodo %d no esta conectado %s:%d\n", nb->base->id, nb->base->red.ip, nb->base->red.puerto);
		}

	}

	//si llego hasta aca significa que las tres copias de bloques que tiene estan desconectadas
	//por lo tanto marco al archivo como no-disponible
	archivo->info->disponible = false;

	return NULL;
}

int fs_marcar_nodo_como_desconectado(t_nodo* nodo){
	printf("El nodo %d - %s:%d se ha desconectado\n", nodo->base->id, nodo->base->red.ip, nodo->base->red.puerto);
	nodo->conectado = false;



	return 0;
}

void fs_print_archivos() {
	printf("IMPRIMIR ARCHIVOS DEL FS \n");
	printf("CANTIDAD DE ARCHIVOS EN EL FS: %d\n", list_size(fs.archivos));
	printf("********************************************\n");

	list_iterate(fs.archivos, (void*) arch_print);
	printf("FIN ARCHIVOS DEL FS ***********************\n");
}

int fs_dir_eliminar_por_id(int id){

	dir_eliminar_por_id(fs.directorios, id);

	return 0;
}

int fs_dir_renombrar(int id, char* nuevo_nombre){
	dir_renombrar(fs.directorios, id, nuevo_nombre);

	return 0;
}

bool fs_existe_dir(char* nombre, int padre) {
	return dir_buscar_por_nombre(fs.directorios, nombre, padre) != NULL;
}


bool fs_existe_dir_por_id(int dir_id) {
	return dir_buscar_por_id(fs.directorios, dir_id) != NULL;
}

int fs_copiar_mdfs_a_local(char* nombre, int dir_id, char* destino) {

	t_archivo* archivo = NULL;
	log_trace(logger, "Comienzo a copiar el archivo");
	//tengo que leer donde esta cada bloque y juntar cada uno para generar un archivo

	//busco el archivo en cuestion
	archivo = fs_buscar_archivo_por_nombre(fs.archivos, nombre, dir_id);

	//obtengo el nombre del archivo destino
	char* nombre_nuevo_archivo = NULL;
	if(string_starts_with(destino, "/tmp")){
		printf("No se puede guardar en tmp\n");
		nombre_nuevo_archivo = strdup(destino);
	}else{
		nombre_nuevo_archivo = file_combine(destino, archivo->info->nombre);
		string_append(&nombre_nuevo_archivo, "_");
		char* timenow = temporal_get_string_time();
		string_append(&nombre_nuevo_archivo, timenow);
		FREE_NULL(timenow);
	}



	//creo el file, si ya existe lo BORRO!!!
	clean_file(nombre_nuevo_archivo);
	FILE* file = txt_open_for_append(nombre_nuevo_archivo);

	int rs=0;
	int numero_bloque;
	char* datos_bloque;
	for (numero_bloque = 0; numero_bloque < archivo->info->cant_bloques; numero_bloque++) {/**/
		//bloque_de_datos = NULL;
		//leo el bloque numero_bloque
		//bloque_de_datos = list_get(archivo->bloques_de_datos, i);

		//le paso la lista de los tres lugares donde esta y me tiene que devolver los datos leidos de cualquiera de lso nodosBloque
		datos_bloque = fs_archivo_get_bloque(nombre, dir_id, numero_bloque);
		if(datos_bloque!=NULL){
			//una vez que traigo el bloque lo grabo en el archivo destino
			txt_write_in_file(file, datos_bloque);
			free(datos_bloque);datos_bloque = NULL;
		}else{
			//si no esta disponible ninguna copia salgo corriendo
			rs = -1;
			break;
		}
	}

	if (rs == -1) {

	} else {
		printf("Creado archivo nuevo: %s\n", nombre_nuevo_archivo);
	}

	txt_close_file(file);
	//FREE_NULL(nombre_nuevo_archivo);
	free(nombre_nuevo_archivo);nombre_nuevo_archivo = NULL;

	return rs;
}
/*
 * me tengo que conectar con el nodo y traer la info, ya sea del 1 2 o 3
 */
char* bloque_de_datos_traer_data(t_list* nodosBloque) {

	return NULL;
}

bool fs_existe_archivo(char* nombre, int dir_id) {

	return fs_buscar_archivo_por_nombre(fs.archivos, nombre, dir_id) != NULL;
}

bool fs_dir_esta_vacio(int id){
	return true;
}

int fs_buscar_directorio_id_por_nombre(char* nombre, int padre){

	t_directorio* dir =  dir_buscar_por_nombre(fs.directorios, nombre, padre);
	if(dir==NULL)
		return -1;
	else
		return dir->index;
}

t_archivo* fs_buscar_archivo_por_nombre(t_list* archivos, char* nombre, int dir_id) {
	t_archivo* archivo = NULL;
	bool _buscar_archivo_por_nombre(t_archivo* archivo) {
		return string_equals_ignore_case(archivo->info->nombre, nombre)
				&& archivo->info->directorio == dir_id;
	}
	archivo = list_find(archivos, (void*) _buscar_archivo_por_nombre);
	return archivo;
}

void fs_print_archivo(char* nombre, int dir_id) {
	t_archivo* archivo = NULL;
	archivo = fs_buscar_archivo_por_nombre(fs.archivos, nombre, dir_id);

	arch_print(archivo);

}

int fs_create() {
	fs.nodos = list_create();
	fs.nodos_no_agregados = list_create();
	fs.directorios = list_create();
	fs.archivos = list_create();



	fs_cargar();


	return 0;
}
void fs_cargar(){
	strcpy(FILE_DIRECTORIO, FILESYSTEM_FILE_DIRECTORIOS());

	if (file_exists(FILE_DIRECTORIO))
		fd_leer_dirs(fs.directorios);
	else
		dir_formatear();

	/*
	if (file_exists(FILE_ARCHIVO) && file_get_size(FILE_ARCHIVO) > 0)
		fd_leer_archivos(fs.archivos);
*/

}


void fs_pasar_nodos_conectados_a_no_agregados(){

	bool _buscar_nodo_conectado(t_nodo* nodo){
		//todo podria mandarse un handshake para verificar que este conectado
		return nodo->conectado;
	}
	void _mover_nodo_a_no_conectado(t_nodo* nodo){
		t_nodo* n = nodo_new(nodo->base->red.ip, nodo->base->red.puerto, true, nodo->cant_bloques, nodo->base->id);

		list_add(fs.nodos_no_agregados, (void*)n);
	}

	t_list* nodos_conectados = list_filter(fs.nodos, (void*)_buscar_nodo_conectado);


	list_iterate(nodos_conectados, (void*)_mover_nodo_a_no_conectado);
	//list_destroy_and_destroy_elements(nodos_conectados, (void*)nodo_destroy);//destroy comun porque la info esta en nodos_no_conectados
	list_destroy(nodos_conectados);

}

void fs_formatear() {
	//FILE* file1 = fopen(FILE_DIRECTORIO, "w+");
	dir_formatear();
	//list_clean(fs.directorios);

	//solo paso los nodos conectados a la lista de nodo_no_agregados
	fs_pasar_nodos_conectados_a_no_agregados();

	list_clean_and_destroy_elements(fs.nodos, (void*) nodo_destroy);

	list_clean(fs.directorios);
	list_clean_and_destroy_elements(fs.archivos, (void*)arch_destroy);


	printf("Formateado!.................................................\n");
	fs_print_nodos_no_agregados();
}

void fs_print_dirs() {
	printf("LISTA DE DIRECTORIOS\n");

	list_iterate(fs.directorios, (void*) dir_print);

	printf("***********************************\n");
}

/*
 * enviar mensaje a los ndos para que se desconecten
 */
void fs_desconectarse() {
	fs_enviar_mensaje_desconexion(fs.nodos);
	fs_enviar_mensaje_desconexion(fs.nodos_no_agregados);
}

void fs_enviar_mensaje_desconexion(t_list* nodos) {
	list_iterate(nodos, (void*) nodo_mensaje_desconexion);
}

t_archivo* fs_buscar_archivo_por_nombre_absoluto(char* path_abs){
	char* dir = malloc(strlen(path_abs)+1);
	char* name = malloc(strlen(path_abs)+1);

	strcpy(dir, path_abs);
	strcpy(name, path_abs);

	dir =	dirname(dir);

	name = basename(name);

	int dir_id = fs_dir_get_index(dir, 0);

	free(dir);dir=NULL;
	//free(name);name=NULL;

	t_archivo* archivo = fs_buscar_archivo_por_nombre(fs.archivos, name, dir_id);
	return archivo;
}

bool fs_existe_nodo_por_ip_puerto(char* ip, int puerto){
	bool _buscar_por_ip_puerto(t_nodo* nodo){
		return strcmp(nodo->base->red.ip, ip)==0 && nodo->base->red.puerto == puerto;
	}

	return list_find(fs.nodos, (void*)_buscar_por_ip_puerto)!=NULL;
}

bool fs_existe_nodo_por_id(int nodo_id) {
	return fs_buscar_nodo_por_id(nodo_id) != NULL;
}

t_nodo* fs_buscar_nodo_por_ip_puerto(char* ip, uint16_t puerto){
	bool _buscar_nodo_por_ip_puerto(t_nodo* nodo){
		return nodo->base->red.puerto == puerto && strcmp(nodo->base->red.ip, ip)==0;
	}
	return list_find ( fs.nodos, (void*)_buscar_nodo_por_ip_puerto);
}
t_nodo* fs_buscar_nodo_por_id(int nodo_id) {
	bool _buscar_nodo_por_id(t_nodo* nodo) {
		return nodo->base->id == nodo_id;
	}
	return list_find(fs.nodos, (void*) _buscar_nodo_por_id);
}


int fs_dir_get_padre(int dir_id){

	if(dir_id == 0 ) return 0;

	t_directorio* dir = dir_buscar_por_id(fs.directorios, dir_id);
	return dir->padre;
}

int fs_dir_get_index(char* path, int padre){
	//si solo pasa el / le devuelvo directamente el raiz
	if(strcmp(path, "/")==0){
		return 0;
	}

	//verifico si esta usando / en el primer char, si es asi uso a padre = 0;
	if(path[0]=='/')
		padre = 0;

	int rs;
	char** nombres = string_split(path, "/");
	char* map = file_get_mapped(FILE_DIRECTORIO);
	int i=0;
	t_directorio* dir = NULL;
	while(nombres[i]!=NULL){
		printf("%s\n", nombres[i]);

		bool _buscar_en_dirs(t_directorio* dir){
			return string_equals_ignore_case(dir->nombre, nombres[i])  && dir->padre == padre;
		}

		dir = list_find(fs.directorios, (void*)_buscar_en_dirs);
		if(dir==NULL){
			rs = -1;
			break;
		}else{
			//asigno el nuevo padre
			padre = dir->index;
			rs = dir->index;
		}

		i++;
	}
	file_mmap_free(map, FILE_DIRECTORIO);
	//limpio el resutlado del split
	i = 0;
	while (nombres[i] != NULL) {
		FREE_NULL(nombres[i]);
		i++;
	}
	FREE_NULL(nombres);

	return rs;
}
char* fs_dir_get_path(int dir_index){

	char* path = string_new();

	if(dir_index==0){
		string_append(&path, "/");
		return path;
	}


	//////////////////////////////////////////
	t_directorio* dir = NULL;
	int i=0;
	do{
		dir = dir_buscar_por_id(fs.directorios, dir_index);

		string_append(&path, dir->nombre);
		string_append(&path, "/");

		dir_index = dir->padre;
		i++;
	}while(dir->padre!=0);

	char** path_split = string_split(path, "/");
	free(path);path = NULL;
	char *rs = string_new();
	int j;
	for(j=1;j<=i;j++){
		string_append(&rs, "/");
		string_append(&rs, path_split[i-j]);
	}

	i = 0;
	while (path_split[i] != NULL) {
		FREE_NULL(path_split[i]);
		i++;
	}
	FREE_NULL(path_split);

	return rs;
}

int fs_agregar_nodo(int id_nodo) {
	//busco el id_nodo en la lista de nodos_nuevos
	bool _buscar_nodo_por_id(t_nodo* nodo) {
		return nodo->base->id == id_nodo;
	}
	//busco el nodo en la lista de no agregados para saber si existe
	t_nodo* nodo;
	if ((nodo = list_find(fs.nodos_no_agregados,(void*) _buscar_nodo_por_id)) == NULL) {
		log_trace(logger, "El nodo ingresado no existe: %d", id_nodo);
		return -1;
	}

	//loborro de la lista de nodos nuevos
	list_remove_by_condition(fs.nodos_no_agregados,(void*) _buscar_nodo_por_id);

	//lo paso a la lista de nodos activos del fs
	list_add(fs.nodos, nodo);
	//nodo->conectado = true;//si descomento lo de abajo comento esta linea

	//le aviso al nodo que esta conectado
	int fd = client_socket(nodo->base->red.ip, nodo->base->red.puerto);
	if((fd<0)){
		log_trace(logger, "No se pudo conectar con el nodo. %s:%d", nodo->base->red.ip, nodo->base->red.puerto);
		nodo->conectado = false;
		return -1;
	}
	else{
		t_msg* msg = argv_message(FS_AGREGO_NODO, 1, nodo->base->id);
		enviar_mensaje(fd, msg);
		destroy_message(msg);
		nodo->conectado = true;

		//si esta conectado verifico si hay algun archivo en el fs que use el nodo para marcar a sus bloques como usados
		void _verificar_uso_de_nodo_en_archivo(t_archivo* archivo){
			void _verificar_uso_de_nodo_enumero_bloque(t_archivo_bloque_con_copias* bloque_de_datos){
				void _verificar_uso_de_nodo_en_copia(t_archivo_nodo_bloque* nodobloque){
					if(nodobloque->base->id == nodo->base->id){
						nodo_marcar_bloque_como_usado(nodo, nodobloque->numero_bloque);
					}

				}
				list_iterate(bloque_de_datos->nodosbloque, (void*)_verificar_uso_de_nodo_en_copia);//en teoria son 3
			}
			list_iterate(archivo->bloques_de_datos, (void*)_verificar_uso_de_nodo_enumero_bloque);
		}
		list_iterate(fs.archivos, (void*)_verificar_uso_de_nodo_en_archivo);

	}
	//le digo que cierre
	enviar_mensaje_nodo_close(fd);
	//close(fd);

	log_trace(logger, "El nodo %d fue agregado al fs", id_nodo);
	//nodo_print_info(nodo);

	return 0;
}

void fs_eliminar_nodo(int id_nodo) {
	t_nodo* nodo = NULL;

	bool buscar_nodo_por_id(t_nodo* nodo) {
		return nodo->base->id == id_nodo;
	}
	nodo = list_find(fs.nodos, (void*) buscar_nodo_por_id);

	//lo saco de la lista
	list_remove_by_condition(fs.nodos, (void*) buscar_nodo_por_id);

	//lo agrego a la lista de nodos no agregados
	list_add(fs.nodos_no_agregados, (void*) nodo);
}

int fs_copiar_archivo_local_al_fs(char* nombre, int dir_padre) {

	t_list* bloques_de_dato = NULL;
	//obtengo los bloques y donde esta guardado cada copia
	bloques_de_dato = fs_importar_archivo(nombre);

	if(bloques_de_dato==NULL){

		return -1;
	}

	//creo el archivo para agregarlo al fs
	t_archivo* archivo = malloc(sizeof *archivo);
	archivo->bloques_de_datos = bloques_de_dato;
	//obtengo info del archivo
	t_archivo_info* info = NULL;info = arch_get_info(nombre, dir_padre);
	info->cant_bloques = list_size(bloques_de_dato);
	archivo->info = info;


	//arch_agregar(archivo);

	//finalmente agrego el archivo a la lista de archivos   DESPUES DE TANTO !!!!!!!!!!!
	list_add(fs.archivos, archivo);

	//printf("el archivo se agrego con id: %d\n", archivo->info->id);

	return 0;
}

/*
 * devuelvo una lista de los t_archivo_bloque_con_copias del archivo
 */
/////////////////////
/*
t_list* fs_importar_archivo(char* archivo) {
	size_t size = file_get_size(archivo);
	printf("Tamanio : %zd b, %.2f kb, %.2f mb\n", size, bytes_to_kilobytes(size),bytes_to_megabytes(size));

	int cant_bloq_necesarios = cant_bloques_necesarios(archivo)*3;
	printf("Bloques necesarios para el archivo: %d\n", cant_bloq_necesarios);
	if(cant_bloq_necesarios> fs_cant_bloques_libres()){
		printf("La cantidad de bloques necesarios para el archivo es %d y solo hay %d bloques libres\n", cant_bloq_necesarios, fs_cant_bloques_libres());
		return NULL;
	}

	t_list* new = list_create();
	t_archivo_bloque_con_copias* bd = NULL;
	int nro_bloque = 0;
	size_t len_keyvalue=LEN_KEYVALUE;
	char* linea = malloc(LEN_KEYVALUE);
	size_t bytes_leidos = 0, offset = 0;

	char* bloque = malloc(TAMANIO_BLOQUE_B);
	FILE* file = fopen(archivo, "r");
	for(;bytes_leidos <size;){
		if(bytes_leidos<TAMANIO_BLOQUE_B){
			//fread(&linea, &len_keyvalue, file);
			bytes_leidos +=strlen(linea);
			//strcat(bloque, linea);
		}else{

			//si supera el tamaño del bloque, grabo
			bd = guardar_bloque(bloque, bytes_leidos);
			bd->numero_bloque = nro_bloque;
			nro_bloque++;
			list_add(new, bd);

			offset = bytes_leidos;
			bytes_leidos = 0;
			memset(bloque, 0, TAMANIO_BLOQUE_B);
		}
	}
	//me fijo si quedo algo sin grabar en el bloque
	if (bytes_leidos > 0) {
		bd = guardar_bloque(bloque, bytes_leidos);

		//setteo el nro de bloque
		bd->numero_bloque = nro_bloque;
		nro_bloque++;
		//agrego el bloquededatos a la lista
		list_add(new, bd);
	}

	free(linea);
	free(bloque);
	fclose(file);

	return new;
}
*/

t_nodo_base* obtener_nodo_con_mayor_cant_bloques_libres(){

	list_sort(fs.nodos, (void*) ordenar_por_mayor_cant_bloques_libres);

	t_nodo* nodo = NULL;
	//obtengo el primero
	nodo = list_get(fs.nodos, 0);

	return nodo->base;
}
/*
 * la copia uno se trata de cargar toda en un mismo nodo,
 * si no se puede tomo el siguiente nodo con mayor cant bloques libres
 */
t_list* cargar_copia_uno(int partes){
	int i;//contar partes
	t_nodo* nodo = NULL;
	t_archivo_nodo_bloque* anb = NULL;
	t_bloque* bloque = NULL;

	t_list* copia_uno = list_create();

	list_sort(fs.nodos, (void*) ordenar_por_mayor_cant_bloques_libres);
	int n_nodo = 0;
	int cant_bloques_libres ;
	i = 0;
	int c_partes=0;
	do{
		nodo = NULL;
		//obtengo el primero
		do {
			nodo = list_get(fs.nodos, n_nodo);
			cant_bloques_libres = nodo_cant_bloques_libres(nodo);
			n_nodo++;
		} while (!nodo_esta_activo(nodo->base));

		//cant_bloques_libres= 1;
		for (i=0; c_partes < partes && i < cant_bloques_libres; i++, c_partes++) {
			//pthread_mutex_lock(&mutex);
			//log_trace(logger, "Nodo: %s b. libres: %d. b. usados: %d",nodo_base_to_string(nodo->base), nodo_cant_bloques_libres(nodo), nodo_cant_bloques_usados(nodo));
			//pthread_mutex_unlock(&mutex);
			//devuelvo un bloque y lo marco como requerido para copiar para que no me traiga
			bloque = nodo_get_bloque_para_copiar(nodo);
			anb = archivo_nodo_bloque_create(nodo->base, bloque->posicion);
			list_add(copia_uno, anb);
		}
	}while(c_partes<partes);

	return copia_uno;

////////////////////////////////////////////////////////////////////

/*
	t_nodo_base* nb = NULL;
	int i;
	t_nodo* nodo = NULL;
	t_archivo_nodo_bloque* anb = NULL;
	t_bloque* bloque = NULL;

	t_list* copia_uno = list_create();
	///////////////////////////////////////////////////
	//cargo la lista que tiene solo la copia 1
	nb = obtener_nodo_con_mayor_cant_bloques_libres();
	nodo = fs_buscar_nodo_por_id(nb->id);
	int cant_bloques_libres = nodo_cant_bloques_libres(nodo);
	for (i = 0; i < partes && i < cant_bloques_libres; i++) {
		//pthread_mutex_lock(&mutex);
		log_trace(logger, "Nodo: %s b. libres: %d. b. usados: %d",nodo_base_to_string(nb), nodo_cant_bloques_libres(nodo), nodo_cant_bloques_usados(nodo));
		//pthread_mutex_unlock(&mutex);
		//devuelvo un bloque y lo marco como requerido para copiar para que no me traiga
		bloque = nodo_get_bloque_para_copiar(nodo);
		anb = archivo_nodo_bloque_create(nb, bloque->posicion);

		list_add(copia_uno, anb);
	}
	if (i > cant_bloques_libres) {
		log_trace(logger, "Nodo: %s no llego toda la c1, le falta llenar %d", nodo_base_to_string(nb), i - cant_bloques_libres);
	}
	return copia_uno;
	*/
}


t_nodo_base* obtener_nodo_mas_cargado_y_distinto_a_nodo(t_nodo_base* otro_nb, int pos_inicial){
	t_nodo* nodo = NULL;

	int pos = list_size(fs.nodos) -1;

	int nodo_get;
	//ordeno
	//list_sort(fs.nodos, (void*)ordenar_por_mayor_cant_bloques_libres);
	bool encontro_nodo = false;
	do{
		do{
			nodo_get = (pos_inicial) % list_size(fs.nodos);
			nodo = list_get(fs.nodos, nodo_get);
			pos_inicial++;;
		} while (!nodo_esta_activo(nodo->base));

		pos--;
		encontro_nodo = !nodo_base_igual_a(*(nodo->base), *(otro_nb)) ;
	}while(!encontro_nodo && pos>=0);
	if(!encontro_nodo){
		//no se encontro una copia que no se repita
		log_trace(logger, "No se encontro una copia distinta al nodo %s", nodo_base_to_string(otro_nb));
		return NULL;
	}

	return nodo->base;
}

t_nodo_base* obtener_nodo_mas_cargado_y_distinto_a_dos_nodos(t_nodo_base* un_nb, t_nodo_base* otro_nb, int pos_inicial){
	t_nodo* nodo = NULL;
	int nodo_get;
	int pos = list_size(fs.nodos)-1;
	//ordeno
	//list_sort(fs.nodos, (void*)ordenar_por_mayor_cant_bloques_libres);
	bool encontro_nodo = false;
	do {
		do{
			nodo_get = (pos_inicial) % list_size(fs.nodos);
			nodo = list_get(fs.nodos, nodo_get);
			pos_inicial++;
		}while(!nodo_esta_activo(nodo->base));

		pos--;
		encontro_nodo = !nodo_base_igual_a(*(nodo->base), *otro_nb) && !nodo_base_igual_a(*(nodo->base), *un_nb);
	} while (!encontro_nodo && pos>=0);
	if(!encontro_nodo){
		//no se encontro una copia que no se repita
		log_trace(logger, "No se encontro una copia disinta al nodo %s y el nodo %d", nodo_base_to_string(un_nb), nodo_base_to_string(otro_nb));
		return NULL;
	}

	return nodo->base;
}


t_list* distribuir_copias(int partes){

	if(list_size(fs.nodos)==0)
		return NULL;

	int i;
	t_list* copia1 = list_create();//lista de t_archivo_nodo_bloque*
	t_list* copia2 = list_create();//lista de t_archivo_nodo_bloque*
	t_list* copia3 = list_create();//lista de t_archivo_nodo_bloque*


	//t_nodo_base* nb = NULL;
	t_nodo* nodo = NULL;
	t_bloque* bloque = NULL;

	copia1 = cargar_copia_uno(partes);

	t_archivo_nodo_bloque* anbc1;
	t_archivo_nodo_bloque* anbc2;
	t_archivo_nodo_bloque* anbc3;
	t_nodo_base* nbc2;
	t_nodo_base* nbc3;
	//ahora lleno la c2 y la c3
	int pos = list_size(fs.nodos)-1;//para que empiece tomando los snodos mas cargados
	//int pos = 0;//init 0 para que el mod sea el max
	//ordeno
	list_sort(fs.nodos, (void*)ordenar_por_menor_cant_bloques_libres);

	for (i = 0; i < partes; i++, pos--) {
		if(pos==-1)//si vuelve a cero lo reinicializo
			pos = list_size(fs.nodos)-1;

		anbc1 = list_get(copia1, i);

		//list_sort(fs.nodos, (void*)ordenar_por_mayor_cant_bloques_libres);

		nbc2 = obtener_nodo_mas_cargado_y_distinto_a_nodo(anbc1->base, pos);
		if(nbc2 == NULL){
			log_trace(logger, "no se pudo cargar la copia 2 de la parte %d del archivo", i);
		return NULL;
		}

		nbc3 = obtener_nodo_mas_cargado_y_distinto_a_dos_nodos(anbc1->base, nbc2, pos);
		if(nbc3 == NULL){
			log_trace(logger, "no se pudo cargar la copia 3 de la parte %d del archivo", i);
			return NULL;
		}
		//cargo c2
		bloque = NULL;
		nodo = NULL;
		nodo = fs_buscar_nodo_por_id(nbc2->id);
		bloque = nodo_get_bloque_para_copiar(nodo);
		anbc2 = archivo_nodo_bloque_create(nbc2, bloque->posicion);
		list_add(copia2, anbc2);

		//cargo c3
		bloque = NULL;
		nodo = NULL;
		nodo = fs_buscar_nodo_por_id(nbc3->id);
		bloque = nodo_get_bloque_para_copiar(nodo);
		anbc3 = archivo_nodo_bloque_create(nbc3, bloque->posicion);
		list_add(copia3, anbc3);

	}
	//t_archivo_nodo_bloque* anb = NULL;
	//abcc = bloque_de_datos_crear();
	printf("Cantidad de partes:%d\nBloques necesarios: %d\n", partes, BLOQUE_CANT_COPIAS*partes);
	////////////////////////////////////////
	void _print_nodo(t_archivo_nodo_bloque* anb){
		printf("nodo:%s, bloque:%d\n", nodo_base_to_string(anb->base), anb->numero_bloque);
		//nodo = fs_buscar_nodo_por_id(anb->base->id);
		//bloque = nodo_buscar_bloque(nodo, anb->numero_bloque);
		//bloque->libre = false;
		//bloque_marcar_como_usado(bloque);
	}
	printf("********************copia 1\n");
	list_iterate(copia1, (void*)_print_nodo);
	printf("********************copia2\n");
	list_iterate(copia2, (void*)_print_nodo);
	printf("********************copia 3\n");
	list_iterate(copia3, (void*)_print_nodo);

	printf("***************************************************************\n");

	//t_archivo_bloque_con_copias* copias = malloc(BLOQUE_CANT_COPIAS*(sizeof(t_archivo_bloque_con_copias*)));
	t_list* copias = list_create();

	t_archivo_bloque_con_copias* abcc;
	t_archivo_nodo_bloque* anb;
	t_nodo_base* nb ;
	t_list* list_archivo_nodo_bloque = list_create();
	for(i=0;i<partes;i++){
		//creo la lista que va tener tres elementos
		list_archivo_nodo_bloque = list_create();

		//obengo uno y lo agrego
		anb = NULL;
		anb = list_get(copia1, i);
		list_add(list_archivo_nodo_bloque, anb);

		//obengo uno y lo agrego
		anb = NULL;
		anb = list_get(copia2, i);
		list_add(list_archivo_nodo_bloque, anb);

		//obengo uno y lo agrego
		anb = NULL;
		anb = list_get(copia3, i);
		list_add(list_archivo_nodo_bloque, anb);

		//creo la parte i con todas sus copias
		abcc = NULL;
		abcc = archivo_bloque_con_copias_crear();
		abcc->nodosbloque = list_archivo_nodo_bloque;
		abcc->parte_numero = i;

		list_add(copias, abcc);
	}

	list_destroy(copia1);
	list_destroy(copia2);
	list_destroy(copia3);

	return copias;
}

t_list* fs_importar_archivo(char* archivo) {
	size_t size = file_get_size(archivo);
	log_trace(logger, "Tamanio : %zd b, %.2f kb, %.2f mb\n", size,	bytes_to_kilobytes(size), bytes_to_megabytes(size));
	int partes = cant_partes_necesarias(archivo);
	int cant_bloq_necesarios = cant_partes_necesarias(archivo) * BLOQUE_CANT_COPIAS;
	log_trace(logger, "Bloques necesarios para el archivo: %d\n", cant_bloq_necesarios);
	if (cant_bloq_necesarios > fs_cant_bloques_libres()) {
		printf(	"La cantidad de bloques necesarios para el archivo es %d y solo hay %d bloques libres\n",cant_bloq_necesarios, fs_cant_bloques_libres());
		return NULL;
	}


	t_list* copias = distribuir_copias(partes);
	if(copias == NULL){
		log_trace(logger, "no se pudieron distribuir las copias");
		return NULL;
	}

	//t_list* new = list_create();

	t_archivo_bloque_con_copias* abcc = NULL;

	char* mapped = file_get_mapped(archivo);

	int parte_nro = 0, rs;
	size_t bytes_leidos = 0, offset = 0;
	int len_aux;
	for (bytes_leidos = 0, offset = 0; bytes_leidos+offset < size; ) {
		len_aux = len_hasta_enter(mapped+bytes_leidos+offset);
		if (bytes_leidos + len_aux < TAMANIO_BLOQUE_B) {
			bytes_leidos += len_aux ;
			//i++;
		} else {
			//si supera el tamaño de bloque grabo
			abcc = list_get(copias, parte_nro);
			rs = guardar_parte(mapped + offset, bytes_leidos, abcc);
			if(rs!=0){
				return NULL;
			}
			//bd = guardar_bloque(mapped + offset, bytes_leidos);
			//bd->parte_numero = parte_nro;
			parte_nro++;
			//list_add(new, bd);

			offset += bytes_leidos;
			bytes_leidos = 0;
		}
	}
	//me fijo si quedo algo sin grabar en el bloque
	if (bytes_leidos > 0) {
		//bd = guardar_parte(mapped + offset, bytes_leidos);
		abcc = list_get(copias, parte_nro);
		rs = guardar_parte(mapped + offset, bytes_leidos, abcc);
		if(rs!=0){
			return NULL;
		}

		//setteo el nro de bloque
		//bd->parte_numero = parte_nro;
		//parte_nro++;
		//agrego el bloquededatos a la lista
		//list_add(new, bd);
	}

	file_mmap_free(mapped, archivo);

	return copias;
}

int guardar_parte_threads(char* bloque_origen,size_t bytes_a_copiar, t_archivo_bloque_con_copias* abcc) {
	int i;
	char* bloque = malloc(bytes_a_copiar+1);
	memcpy(bloque, bloque_origen, bytes_a_copiar);
	bloque[bytes_a_copiar] = '\0';
	t_archivo_nodo_bloque* anb = NULL;
	//me traigo en un vector los tres t_nodo_bloque donde va a ir la copia del bloque
	//nb = fs_get_tres_nodo_bloque_libres(fs);

	int _set_bloque(t_archivo_nodo_bloque* anb){
		fs_guardar_bloque(anb, bloque, bytes_a_copiar);

		t_nodo* nodo = fs_buscar_nodo_por_id(anb->base->id);

		nodo_marcar_bloque_como_usado(nodo, anb->numero_bloque);

		pthread_mutex_lock(&mutex);
		log_trace(logger, "nodo %d bloque %d marcado como usado", anb->base->id, anb->numero_bloque);
		pthread_mutex_unlock(&mutex);
		return 0;
	}
	pthread_t th_copia[BLOQUE_CANT_COPIAS];

	for (i = 0; i < BLOQUE_CANT_COPIAS; i++) {
		anb = NULL;
		anb = list_get(abcc->nodosbloque, i);
		if (!nodo_esta_activo(anb->base)) {
			pthread_mutex_lock(&mutex);
			log_trace(logger, "Nodo %s NO activo", anb->base);
			pthread_mutex_unlock(&mutex);
			return -1;
		}
		pthread_create(&(th_copia[i]), NULL, (void*)_set_bloque, NULL);
	}
	int th_rs[BLOQUE_CANT_COPIAS];
	for (i = 0; i < BLOQUE_CANT_COPIAS; i++) {
		pthread_join(th_copia[BLOQUE_CANT_COPIAS], (void*)&(th_rs[i]));
	}

	FREE_NULL(bloque);
	return 0;
}


int guardar_parte(char* bloque_origen,size_t bytes_a_copiar, t_archivo_bloque_con_copias* abcc) {
	int i;
	char* bloque = malloc(bytes_a_copiar+1);
	memcpy(bloque, bloque_origen, bytes_a_copiar);
	bloque[bytes_a_copiar] = '\0';
	t_archivo_nodo_bloque* anb = NULL;
	//me traigo en un vector los tres t_nodo_bloque donde va a ir la copia del bloque
	//nb = fs_get_tres_nodo_bloque_libres(fs);


	///////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////

	for (i = 0; i < BLOQUE_CANT_COPIAS; i++) {

		anb = NULL;
		anb = list_get(abcc->nodosbloque, i);
		if(!nodo_esta_activo(anb->base)){
			log_trace(logger, "Nodo %s activo", anb->base);
			return -1;
		}

		fs_guardar_bloque(anb, bloque, bytes_a_copiar);

		t_nodo* nodo = fs_buscar_nodo_por_id(anb->base->id);

		nodo_marcar_bloque_como_usado(nodo, anb->numero_bloque);

		log_trace(logger, "nodo %d bloque %d marcado como usado\n", anb->base->id,anb->numero_bloque);

		//agrego el bloquenodo a la lista, al final del for quedaria con las tres copias y faltaria settear el nro_bloque
		//list_add(new->nodosbloque, (void*) nb[i]);
	}
	//hacer free de lam matriz

	FREE_NULL(bloque);
	return 0;
}


void bloque_marcar_como_usado(t_bloque* bloque) {
	bloque->libre = false;
	bloque->requerido_para_copia = true;
}

t_archivo_bloque_con_copias* guardar_bloque(char* bloque_origen,size_t bytes_a_copiar) {
	t_archivo_bloque_con_copias* new = archivo_bloque_con_copias_crear();

	int i;
	//t_bloque* bloque_usado;
	//genero el bloque a copiar
	//t_nodo* nodo;
	char* bloque = malloc(bytes_a_copiar+1);
	memcpy(bloque, bloque_origen, bytes_a_copiar);
	bloque[bytes_a_copiar] = '\0';
	t_archivo_nodo_bloque** nb = NULL;
	//me traigo en un vector los tres t_nodo_bloque donde va a ir la copia del bloque
	nb = fs_get_tres_nodo_bloque_libres(fs);
	for (i = 0; i < BLOQUE_CANT_COPIAS; i++) {
		fs_guardar_bloque(nb[i], bloque, bytes_a_copiar);

		t_nodo* nodo = fs_buscar_nodo_por_id(nb[i]->base->id);

		nodo_marcar_bloque_como_usado(nodo, nb[i]->numero_bloque);
		/*
		bool buscar_bloque(t_bloque* bloque) {
			return bloque->posicion == nb[i]->numero_bloque;
		}
		//busco el nodo por id para obtener su lista de bloques
		nodo = fs_buscar_nodo_por_id(nb[i]->nodo->id);

		bloque_usado = list_find(nodo->bloques, (void*) buscar_bloque);
		bloque_marcar_como_usado(bloque_usado);
		*/


		//log_trace(logger, "nodo %d bloque %d marcado como usado\n", nb[i]->base->id,nb[i]->numero_bloque);

		//agrego el bloquenodo a la lista, al final del for quedaria con las tres copias y faltaria settear el nro_bloque
		list_add(new->nodosbloque, (void*) nb[i]);
	}
	free(nb);
	//hacer free de lam matriz

	FREE_NULL(bloque);

	return new;
}

int fs_guardar_bloque(t_archivo_nodo_bloque* nb, char* bloque, size_t tamanio_real) {
	//me tengo que conectar con el nodo y pasarle el bloque
	//obtengo info del bloque
	int rs ;
	pthread_mutex_lock(&mutex_log);
	log_trace(logger, "Iniciando transferencia a Ip:%s:%d bloque %d", nb->base->red.ip,nb->base->red.puerto, nb->numero_bloque);
	pthread_mutex_unlock(&mutex_log);
	int fd = client_socket(nb->base->red.ip, nb->base->red.puerto);

	//le digo que grabe el blque en el nodo n
	t_msg* msg = string_message(FS_GRABAR_BLOQUE, bloque, 2, nb->numero_bloque, tamanio_real);

	rs = enviar_mensaje(fd, msg);
	destroy_message(msg);

	rs = recibir_mensaje_nodo_ok(fd);

	//enviar_mensaje_nodo_close(fd);

	close(fd);

	pthread_mutex_lock(&mutex_log);
	log_trace(logger, "transferencia realizada OK");
	pthread_mutex_unlock(&mutex_log);
	return rs;
}

void fs_print_no_operativo(){
	printf("FS no operativo. \n");
	printf("Cantidad minima de nodos conectados: %d\n", fs.cant_nodos_minima);
	printf("Cantidad de nodos conectados: %d\n", list_size(fs.nodos));
	printf("Cantidad de nodos no conectados: %d\n",
			list_size(fs.nodos_no_agregados));
}

void fs_print_nodos_no_agregados() {
	printf("Cantidad nodos no agregados al sistema (addnodo n para agregarlos): %d\n", list_size(fs.nodos_no_agregados));
	list_iterate(fs.nodos_no_agregados, (void*) print_nodo);
	printf("\n---------------------------------------------------");
}
/*
 void fs_print_nodos(t_list* nodos) {
 printf("NODOS CONECTADOS AL SISTEMA ('addnodo n' para agregarlos)\n");
 list_iterate(nodos, (void*) print_nodo);
 printf("\n---------------------------------------------------");
 }*/

bool ordenar_por_menor_cant_bloques_libres(t_nodo* uno, t_nodo* dos) {
	return nodo_cant_bloques_libres(uno) < nodo_cant_bloques_libres(dos);
}

bool ordenar_por_mayor_cant_bloques_libres(t_nodo* uno, t_nodo* dos) {
	return nodo_cant_bloques_libres(uno) > nodo_cant_bloques_libres(dos);
}


t_list* obtener_tres_nodos_disponibles() {
	//ordeno por cantidad de libres

	if (list_size(fs.nodos) >= 3) {
		list_sort(fs.nodos, (void*) ordenar_por_mayor_cant_bloques_libres);
		//genera una lista nueva y la devuelvo
		return list_take(fs.nodos, 3);
	} else {
		//si es menor a tres agarro el primer nodo , verificar si es posible con el ayudante
		//guardaria todo el el mismo nodo y distinto bloque
		t_list* lista = list_create();

		//tomo el primer nodo porque si!, consultar con ayte si si puede pasar que el fs quede con menso de tres nodos y que hacer ....
		list_add(lista, (t_nodo*) list_get(fs.nodos, 0));
		list_add(lista, (t_nodo*) list_get(fs.nodos, 0));
		list_add(lista, (t_nodo*) list_get(fs.nodos, 0));

		return lista;
	}

	return fs.nodos;
}
/*
 * devuelvo un vector de tres posiciones con la info de donde va cada copia del bloque
 */
t_archivo_nodo_bloque** fs_get_tres_nodo_bloque_libres() {
	t_archivo_nodo_bloque** new = NULL;

	t_list* nodos_destino = obtener_tres_nodos_disponibles(fs);

	/*
	 * todo lo que sigue es crear el vector con los tres t_nodo_bloque
	 */
	//inicializo la matriz de 3 elementos tipo t_nodo_bloque
	new = malloc(BLOQUE_CANT_COPIAS * sizeof(t_archivo_nodo_bloque*));

	t_bloque* bloque;
	t_nodo* nodo = NULL;
	//tomo los tres primeros
	int i = 0;
	for (i = 0; i < 3; i++) {
		//reservo espacio para el nodo_bloque
		new[i] = malloc(sizeof(t_archivo_nodo_bloque));

		nodo = NULL;
		//tomo el de la posicion i porque la lsita ya esta ordenada por mayor cant de bloques libres
		nodo = (t_nodo*) list_get(nodos_destino, i);
		bloque = NULL;

		//devuelvo un bloque y lo marco como requerido para copiar para que no me traiga
		bloque = nodo_get_bloque_para_copiar(nodo);

		new[i]->base = nodo->base;
		/*
		new[i]->base->id = nodo->base->id;
		strcpy(new[i]->base->red.ip, nodo->base->red.ip);
		new[i]->base->red.puerto = nodo->base->red.puerto;
		*/

		new[i]->numero_bloque = bloque->posicion;

	}
	list_destroy(nodos_destino);

	return new;
}


/*
 * devuelvo 20mb con el pedazo de bloque leido hasta el \n
 */
char* file_obtener_bloque(char* mapped, int numero_bloque) {
	return NULL;
}

int cant_partes_necesarias(char* archivo) {
	int cant = 0;
	size_t size = file_get_size(archivo);

	cant = size / TAMANIO_BLOQUE_B;

//si no es multiplo del tamaño del bloque sumo 1
	if (size % TAMANIO_BLOQUE_B != 0)
		cant++;

	return cant;
}

void fs_print_info() {
	printf("INFORMACION ACTUAL DEL FS\n");

	//size_t tam = fs_tamanio_bytes(fs);
	//size_t tam_libre = fs_tamanio_libre_bytes(fs);
	//size_t tam_usado = fs_tamanio_usado_bytes(fs);

	//printf("Tamanio : %zd b, %.2f kb, %.2f mb\n", tam, bytes_to_kilobytes(tam), bytes_to_megabytes(tam));
	//printf("Tamanio : %.2f kb", bytes_to_kilobytes(fs_tamanio_bytes(fs)));
	//printf("Tamanio : %.2f mb\n", bytes_to_megabytes(fs_tamanio_bytes(fs)));
	//printf("Tamanio : %.2f mb\n", bytes_to_megabytes(fs_tamanio_bytes(fs)));
	//printf("Tamanio : %d MB\n", fs_tamanio_megabytes(fs));
	//printf("Tamanio libre: %zd b, %.2f kb, %.2f mb\n", tam_libre,bytes_to_kilobytes(tam_libre), bytes_to_megabytes(tam_libre));
	//printf("Tamanio usado: %zd b, %.2f kb, %.2f mb\n", tam_usado, bytes_to_kilobytes(tam_usado), bytes_to_megabytes(tam_usado));

	//printf("Tamanio libre: %d MB\n", fs_tamanio_libre_megabytes(fs));
	//printf("Tamanio usado: %d MB\n", fs_tamanio_usado_megabytes(fs));

	printf("Cant Nodos conectados: %d\n", list_size(fs.nodos));
	printf("Cant Nodos no agregados: %d\n", list_size(fs.nodos_no_agregados));

	printf("Tamaño bloque %d mb\n", TAMANIO_BLOQUE_MB);
	printf("Cant bloques totales %d\n", fs_cant_bloques(fs));
	printf("Cant bloques libres: %d\n", fs_cant_bloques_libres(fs));
	printf("Cant bloques usados: %d\n", fs_cant_bloques_usados(fs));

	printf("*********************************************\n");
}

int fs_cant_bloques_usados() {
	return fs_cant_bloques() - fs_cant_bloques_libres();
}

int cant_bloques_usados() {
	return fs_cant_bloques() - fs_cant_bloques_libres();
}

void fs_addNodo(t_nodo* nodo) {
	list_add(fs.nodos, nodo);

//actualizo el tamaño del nodo
//fs.cant_bloques = fs.cant_bloques + CANT_BLOQUES;
}

int fs_cant_bloques_libres() {
	int cant = 0;

	void cant_bloques_libres(t_nodo* nodo) {
		cant += nodo_cant_bloques_libres(nodo);
	}

	list_iterate(fs.nodos, (void*) cant_bloques_libres);

	return cant;
}

int fs_cant_bloques() {
	size_t size = 0;

	void contar_bloques(t_nodo* nodo) {
		size += nodo_cant_bloques(nodo);
	}

	list_iterate(fs.nodos, (void*) contar_bloques);

	return size;
}

size_t fs_tamanio_usado_bytes() {
	return fs_cant_bloques_usados() * TAMANIO_BLOQUE_B;
}

size_t fs_tamanio_libre_bytes() {
	return fs_cant_bloques_libres() * TAMANIO_BLOQUE_B;
}

size_t fs_tamanio_bytes() {
	return fs_cant_bloques() * TAMANIO_BLOQUE_B;
}


void fd_leer_dirs(t_list* dirs) {
	char* map = file_get_mapped(FILE_DIRECTORIO);
	t_directorio* dir;
	dir = malloc(sizeof *dir);
	int i = 0;
	for (i = 0; i < DIR_CANT_MAX; i++) {

		dir = memcpy(dir, map + (i * sizeof(t_directorio)),	sizeof(t_directorio));
		if (dir->index != 0) { //si es igual a 0 esta disponible
			list_add(dirs, (void*) dir);
			dir = malloc(sizeof *dir);
		}
	}
	FREE_NULL(dir);
	file_mmap_free(map, FILE_DIRECTORIO);
}

void fs_destroy() {
	list_destroy_and_destroy_elements(fs.nodos, (void*) nodo_destroy);
	list_destroy_and_destroy_elements(fs.nodos_no_agregados, (void*) nodo_destroy);
	list_destroy_and_destroy_elements(fs.directorios, (void*)dir_destroy);
	list_destroy_and_destroy_elements(fs.archivos, (void*)arch_destroy);
}

#endif /* FILESYSTEM_H_ */

