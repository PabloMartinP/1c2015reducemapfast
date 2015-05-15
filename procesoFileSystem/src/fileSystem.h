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
#include <commons/temporal.h>


char FILE_NODOS[1024] =	"/mdfs_nodos.bin";

typedef struct {
	char** nodos_necesarios;
	bool operativo;
	t_list* nodos;
	t_list* nodos_no_agregados; //son los nodos conectados pero no agregados al fs
	t_list* directorios;
	t_list* archivos;

} t_fileSystem;

t_fileSystem fs;

const int BLOQUE_CANT_COPIAS = 3;

void fs_print_dirs();
void fs_agregar_nodo(int id_nodo);
void fs_create();
void fs_destroy();
void fs_addNodo(t_nodo* nodo);
void fs_print_info();
int fs_cant_bloques();
size_t fs_tamanio_bytes();
int fs_cant_bloques_libres();
int fs_cant_bloques_usados();
int cant_bloques_necesarios(char* archivo);
char* file_obtener_bloque(char* mapped, int n_bloque);

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
void fs_copiar_archivo_local_al_fs(char* archivo,int dir_padre);
void fs_print_archivo(char* nombre, int dir_id);
t_archivo* fs_buscar_archivo_por_nombre(t_list* archivos, char* nombre,	int dir_id);
bool fs_existe_archivo(char* nombre, int dir_id);
void fs_copiar_mdfs_a_local(char* nombre, int dir_id,
		char* destino);
bool fs_existe_dir(int dir_id);
void fs_print_archivos();
char* bloque_de_datos_traer_data(t_list* nodosBloque);
char* fs_archivo_get_bloque(char* nombre, int dir_id,	int n_bloque);
void fs_archivo_ver_bloque(char* nombre, int dir_id,	int n_bloque);
bool fs_esta_operativo();
bool fs_existe_nodo_por_id(int nodo_id);
void fs_cargar_nodo_base(t_nodo_base* destino, int nodo_id);
t_nodo* fs_buscar_nodo_por_ip_puerto(char* ip, uint16_t puerto);
int fs_get_nodo_id_en_archivo_nodos(char* ip, uint16_t puerto);
void fs_cargar();
/*
 * ****************************************************************************************
 */

void fs_cargar_nodo_base(t_nodo_base* destino, int nodo_id){
	char* map = file_get_mapped(FILE_NODOS);

	//busco el registro, SIEMPRE VA EXISTIR
	int cant_reg = file_get_size(FILE_NODOS) / sizeof(t_nodo_base);
	int i;
	for (i = 0; i < cant_reg; i++) {
		memcpy(destino, map + i*sizeof(t_nodo_base), sizeof(t_nodo_base));
		if(destino->id == nodo_id){
			//si es el que busco salgo del for
			break;
		}

	}

	file_mmap_free(map, FILE_NODOS);
}
bool fs_esta_operativo() {
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

	return fs.operativo;

}

void fs_archivo_ver_bloque(char* nombre, int dir_id, int n_bloque) {
	char* datos_bloque = NULL;

	if ((datos_bloque = fs_archivo_get_bloque(nombre, dir_id, n_bloque))
			!= NULL) {
		printf("Inicio bloque %d del archivo %s\n", n_bloque, nombre);
		printf("%s", datos_bloque);
		printf("\n***********************************************");
		printf("Fin bloque %d del archivo %s\n", n_bloque, nombre);
	}

}
char* fs_archivo_get_bloque(char* nombre, int dir_id,
		int n_bloque) {
	int i;
	//busco el archivo
	t_archivo* a = NULL;
	a = fs_buscar_archivo_por_nombre(fs.archivos, nombre, dir_id);

	//busco el bloque
	t_bloque_de_datos* bloque = NULL;
	bloque = arch_buscar_bloque(a, n_bloque);

	//tengo que verificar si alguno de los nodos que tiene la copia esta disponible
	//hardcodeo ip y puerto
	//char* ip = "127.0.0.1";
	//int puerto = 6001;

	t_nodo_bloque* nb = NULL;

	char* datos_bloque = NULL;

	for (i = 0; i < list_size(bloque->nodosbloque); i++) {
		nb = NULL;
		nb = list_get(bloque->nodosbloque, i);

		//obtengo el ip y puerto en base al nodo_id
		if (bloque->n_bloque == n_bloque && nodo_esta_vivo(nb->nodo->base.ip, nb->nodo->base.puerto)) {

			//si esta vivo inicio la transferencia del bloque
			nb = NULL;
			nb = list_get(bloque->nodosbloque, i);

			//me conecto con el nodo
			int fd = client_socket(nb->nodo->base.ip, nb->nodo->base.puerto);
			//le pido el bloque n_bloque
			t_msg* msg = string_message(NODO_GET_BLOQUE, "", 1, nb->n_bloque);
			enviar_mensaje(fd, msg);
			destroy_message(msg);
			msg = recibir_mensaje(fd);
			//print_msg(msg);

			//copy los datos del stream a una copia para devolver
			datos_bloque = malloc(msg->header.length);
			memccpy(datos_bloque, msg->stream, 1, msg->header.length);

			destroy_message(msg);

			break;
		}
	}

	return datos_bloque;
}

void fs_print_archivos() {
	printf("IMPRIMIR ARCHIVOS DEL FS \n");
	printf("CANTIDAD DE ARCHIVOS EN EL FS: %d\n", list_size(fs.archivos));
	printf("********************************************\n");

	list_iterate(fs.archivos, (void*) arch_print);
	printf("FIN ARCHIVOS DEL FS ***********************\n");
}

bool fs_existe_dir(int dir_id) {
	return dir_buscar_por_id(fs.directorios, dir_id) != NULL;
}

void fs_copiar_mdfs_a_local(char* nombre, int dir_id, char* destino) {
	int i;
	t_archivo* archivo = NULL;
	printf("comienzo a exportar el archivo\n");
	//tengo que leer donde esta cada bloque y juntar cada uno para generar un archivo

	//busco el archivo en cuestion
	archivo = fs_buscar_archivo_por_nombre(fs.archivos, nombre, dir_id);

	//obtengo el nombre del archivo destino
	char* nombre_nuevo_archivo = NULL;
	nombre_nuevo_archivo = file_combine(destino, archivo->info->nombre);
	char* timenow = temporal_get_string_time();
	string_append(&nombre_nuevo_archivo, timenow);
	free_null((void*)&timenow);
	//creo el file, si ya existe lo BORRO!!!
	clean_file(nombre_nuevo_archivo);
	FILE* file = txt_open_for_append(nombre_nuevo_archivo);

	t_bloque_de_datos* bloque_de_datos = NULL;
	char* datos_bloque;
	for (i = 0; i < archivo->info->cant_bloques; i++) {/**/
		bloque_de_datos = NULL;
		//leo el bloque i
		bloque_de_datos = list_get(archivo->bloques_de_datos, i);

		//le paso la lista de los tres lugares donde esta y me tiene que devolver los datos leidos de cualquiera de lso nodosBloque
		datos_bloque = fs_archivo_get_bloque(nombre, dir_id, i);

		//una vez que traigo el bloque lo grabo en un archivo
		txt_write_in_file(file, datos_bloque);
	}

	printf("Creado archivo nuevo: %s\n", nombre_nuevo_archivo);
	txt_close_file(file);
	free_null((void*)&nombre_nuevo_archivo);
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

t_archivo* fs_buscar_archivo_por_nombre(t_list* archivos, char* nombre,
		int dir_id) {
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

void fs_create() {



	fs.nodos = list_create();

	fs.nodos_no_agregados = list_create();

	fs.directorios = list_create();
	fs.archivos = list_create();



	fs_cargar();

}
void fs_cargar(){
	if (file_exists(FILE_DIRECTORIO))
		fd_leer_dirs(fs.directorios);

	if (file_exists(FILE_ARCHIVO) && file_get_size(FILE_ARCHIVO) > 0)
		fd_leer_archivos(fs.archivos);

	ID_NODO_NUEVO = 0;
	if (!file_exists(FILE_NODOS))
		clean_file(FILE_NODOS);
	else
		ID_NODO_NUEVO = (file_get_size(FILE_NODOS) / sizeof(t_nodo_base));

	fs.operativo = false;
}

void fs_pasar_nodos_conectados_a_no_agregados(){

	bool _buscar_nodo_conectado(t_nodo* nodo){
		//todo podria mandarse un handshake para verificar que este conectado
		return nodo->conectado;
	}
	void _mover_nodo_a_no_conectado(t_nodo* nodo){
		//tengo que asignarles un id y meter el nodo en la lista de no-conectados
		t_nodo* n = nodo_new(nodo->base.ip, nodo->base.puerto, true, nodo->base.cant_bloques);
		//nodo->base.id = nodo_get_new_nodo_id();
		//nodo->conectado = false;
		n->base.id = nodo_get_new_nodo_id();
		//nodo_marcar_como_libre_total(nodo);

		list_add(fs.nodos_no_agregados, (void*)n);
	}

	t_list* nodos_conectados = list_filter(fs.nodos, (void*)_buscar_nodo_conectado);


	list_iterate(nodos_conectados, (void*)_mover_nodo_a_no_conectado);
	//list_destroy_and_destroy_elements(nodos_conectados, (void*)nodo_destroy);//destroy comun porque la info esta en nodos_no_conectados
	list_destroy(nodos_conectados);

}

void fs_formatear() {
	clean_file(FILE_NODOS);
	//FILE* file1 = fopen(FILE_DIRECTORIO, "w+");
	dir_formatear();
	//list_clean(fs.directorios);

	ID_NODO_NUEVO = 0;//vuelvo a cero el contador para que el metod siguiente asigne ids desde el inicio
	//solo paso los nodos conectados a la lista de nodo_no_agregados
	fs_pasar_nodos_conectados_a_no_agregados();


	//list_clean(fs.nodos);

	arch_formatear();
	//list_clean_and_destroy_elements(fs.archivos, (void*)arch_destroy);

	clean_file(FILE_NODOS);
	//ID_NODO_NUEVO = 0;

	//fs_destroy();

	list_clean_and_destroy_elements(fs.nodos, (void*) nodo_destroy);
	//list_destroy(fs.nodos);

	/*
	bool __id(t_nodo* nodo){
		return nodo->base.id == 1;
	}
	t_nodo* n = list_find(fs.nodos, (void*)__id);*/

	list_clean(fs.directorios);
	list_clean_and_destroy_elements(fs.archivos, (void*)arch_destroy);

	//fs_create();

	////////////////////////
	fs.operativo = false;
	//fs_cargar();

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
bool fs_existe_nodo_por_id(int nodo_id) {
	return fs_buscar_nodo_por_id(nodo_id) != NULL;
}

t_nodo* fs_buscar_nodo_por_ip_puerto(char* ip, uint16_t puerto){
	bool _buscar_nodo_por_ip_puerto(t_nodo* nodo){
		return nodo->base.puerto == puerto && strcmp(nodo->base.ip, ip)==0;
	}
	return list_find ( fs.nodos, (void*)_buscar_nodo_por_ip_puerto);
}
t_nodo* fs_buscar_nodo_por_id(int nodo_id) {
	bool _buscar_nodo_por_id(t_nodo* nodo) {
		return nodo->base.id == nodo_id;
	}
	return list_find(fs.nodos, (void*) _buscar_nodo_por_id);
}
/*
 * leo en el archivo de nodos y busco si el id esta, si no esta
 */
int fs_get_nodo_id_en_archivo_nodos(char* ip, uint16_t puerto){
	if (file_get_size(FILE_NODOS) > 0) {
		int id = 0;
		size_t size = file_get_size(FILE_NODOS);
		//tengo la cantidad de nodos
		int cant_reg = size / sizeof(t_nodo_base);
		//si hay info, busco si ya existe el nodo_base
		char* map = file_get_mapped(FILE_NODOS);
		t_nodo_base nb;
		int i;
		for (i = 0; i < cant_reg; i++) {
			//leo el registro del nodo
			memcpy(&nb, map + i * sizeof(t_nodo_base), sizeof(t_nodo_base));

			if(strcmp(nb.ip, ip)==0 && nb.puerto == puerto){
				//guardo el id para devolverlo
				id = nb.id;
				break;
			}
		}

		file_mmap_free(map, FILE_NODOS);
		return id;

	} else
		return nodo_get_new_nodo_id();	//si el tamaño es cero no hay ningun nodo guardado
}

bool fs_existe_en_archivo_nodos(t_nodo_base base){
	if(file_get_size(FILE_NODOS)>0){
		bool encontro = false;
		size_t size = file_get_size(FILE_NODOS);
		//tengo la cantidad de nodos
		int cant_reg = size / sizeof(t_nodo_base);
		//si hay info, busco si ya existe el nodo_base
		char* map = file_get_mapped(FILE_NODOS);
		t_nodo_base nb;
		int i;
		for (i = 0; i < cant_reg; i++) {
			//leo el registro del nodo
			memcpy(&nb, map + i * sizeof(t_nodo_base), sizeof(t_nodo_base));

			if(nodo_base_igual_a(base, nb)){
				encontro = true;
				break;
			}
		}
		file_mmap_free(map, FILE_NODOS);

		return encontro;
	}
	else
		return false;//si el tamaño es cero no hay ningun nodo guardado
}

void fs_agregar_nodo(int id_nodo) {
	//busco el id_nodo en la lista de nodos_nuevos
	bool _buscar_nodo_por_id(t_nodo* nodo) {
		return nodo->base.id == id_nodo;
	}
	//busco el nodo en la lista de no agregados para saber si existe
	t_nodo* nodo;
	if ((nodo = list_find(fs.nodos_no_agregados,(void*) _buscar_nodo_por_id)) == NULL) {
		printf("El nodo ingresado no existe: %d\n", id_nodo);
		return;
	}

	//ahora tengo que agregarlo al archivo nodos.bin
	//busco si ya existe en la tabla, en caso de que exista no hago nada
	/////////////////////////////////////


	if (!fs_existe_en_archivo_nodos(nodo->base)) {
		//si no existe lo agrego

		t_nodo_base nb;
		//cargo la estructura para guardarla en el file
		nb.id = nodo->base.id;
		memset(nb.ip, '\0', sizeof(nb.ip));//setteo a  \0 por las d uda
		strcpy(nb.ip, nodo->base.ip);
		nb.puerto = nodo->base.puerto;
		nb.cant_bloques = nodo->base.cant_bloques;


		//grabo en el archivo de nodos
		FILE* file = txt_open_for_append(FILE_NODOS);
		fwrite(&nb, sizeof(t_nodo_base), 1, file);
		txt_close_file(file);
	}

	//loborro de la lista de nodos nuevos
	list_remove_by_condition(fs.nodos_no_agregados,(void*) _buscar_nodo_por_id);

	//lo paso a la lista de nodos activos del fs
	//antes de pasarlo verifico que no exista, puede haberse insertado mientras leia los bloques de los archivos

	t_nodo* nn = NULL;
	nn = list_find(fs.nodos, (void*)_buscar_nodo_por_id);
	//solo lo agrego a la lista de nodos si no existe
	if(nn==NULL){
		list_add(fs.nodos, nodo);
		nn = nodo;//se lo asigno para poder hacer el printf y el conectado = true;
	}
	nn->conectado = true;


	printf("El nodo fue agregado al fs: \n");
	nodo_print_info(nn);

}

void fs_eliminar_nodo(int id_nodo) {
	t_nodo* nodo = NULL;

	bool buscar_nodo_por_id(t_nodo* nodo) {
		return nodo->base.id == id_nodo;
	}
	nodo = list_find(fs.nodos, (void*) buscar_nodo_por_id);

	//lo saco de la lista
	list_remove_by_condition(fs.nodos, (void*) buscar_nodo_por_id);

	//lo agrego a la lista de nodos no agregados
	list_add(fs.nodos_no_agregados, (void*) nodo);
}

void fs_copiar_archivo_local_al_fs(char* nombre,
		int dir_padre) {

	t_list* bloques_de_dato = NULL;
	//obtengo los bloques y donde esta guardado cada copia
	bloques_de_dato = fs_importar_archivo(nombre);

	//obtengo info del archivo
	t_archivo_info* info = arch_get_info(nombre, dir_padre);

	//creo el archivo para agregarlo al fs
	t_archivo* archivo = malloc(sizeof *archivo);
	archivo->bloques_de_datos = bloques_de_dato;
	archivo->info = info;

	//lo agrego al fs
	arch_agregar(archivo);

	//finalmente agrego el archivo a la lista de archivos   DESPUES DE TANTO !!!!!!!!!!!
	list_add(fs.archivos, archivo);

	printf("el archivo se agrego con id: %d\n", archivo->info->id);
}

/*
 * devuelvo una lista de los t_bloque_de_datos del archivo
 */
t_list* fs_importar_archivo(char* archivo) {

	t_list* new = list_create();

	t_bloque_de_datos* bd = NULL;

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

	for (i = 0; i < c_registros; ) {

		if (bytes_leidos + strlen(registros[i]) + 1 < TAMANIO_BLOQUE_B) {
			bytes_leidos += strlen(registros[i]) + 1;
			i++;
		} else {
			//si supera el tamaño de bloque grabo

			bd = guardar_bloque(mapped + offset, bytes_leidos);
			bd->n_bloque = nro_bloque;
			nro_bloque++;
			list_add(new, bd);

			offset = bytes_leidos;
			bytes_leidos = 0;
		}
	}
	//me fijo si quedo algo sin grabar en el bloque
	if (bytes_leidos > 0) {
		bd = guardar_bloque(mapped + offset, bytes_leidos);
		//setteo el nro de bloque
		bd->n_bloque = nro_bloque;
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

t_bloque_de_datos* guardar_bloque(char* bloque_origen,size_t bytes_a_copiar) {
	t_bloque_de_datos* new = bloque_de_datos_create();

	int i;
	//t_bloque* bloque_usado;
	//genero el bloque a copiar
	//t_nodo* nodo;
	char* bloque = malloc(bytes_a_copiar);
	memcpy(bloque, bloque_origen, bytes_a_copiar);

	t_nodo_bloque** nb = NULL;
	//me traigo en un vector los tres t_nodo_bloque donde va a ir la copia del bloque
	nb = fs_get_tres_nodo_bloque_libres(fs);
	for (i = 0; i < BLOQUE_CANT_COPIAS; i++) {
		fs_guardar_bloque(nb[i], bloque, bytes_a_copiar);

		nodo_marcar_bloque_como_usado(nb[i]->nodo, nb[i]->n_bloque);
		/*
		bool buscar_bloque(t_bloque* bloque) {
			return bloque->posicion == nb[i]->n_bloque;
		}
		//busco el nodo por id para obtener su lista de bloques
		nodo = fs_buscar_nodo_por_id(nb[i]->nodo->id);

		bloque_usado = list_find(nodo->bloques, (void*) buscar_bloque);
		bloque_marcar_como_usado(bloque_usado);
		*/


		printf("nodo %d bloque %d marcado como usado\n", nb[i]->nodo->base.id,nb[i]->n_bloque);

		//agrego el bloquenodo a la lista, al final del for quedaria con las tres copias y faltaria settear el nro_bloque
		list_add(new->nodosbloque, (void*) nb[i]);
	}
	//free(nb)
	//hacer free de lam matriz

	free_null((void*)&bloque);

	return new;
}

void fs_guardar_bloque(t_nodo_bloque* nb, char* bloque, size_t tamanio_real) {
	//me tengo que conectar con el nodo y pasarle el bloque
	//obtengo info del bloque
	t_nodo* nodo = NULL;
	nodo = fs_buscar_nodo_por_id(nb->nodo->base.id);

	printf("iniciando transferencia a Ip:%s:%d bloque %d\n", nodo->base.ip,nodo->base.puerto, nb->n_bloque);
	int fd = client_socket(nodo->base.ip, nodo->base.puerto);

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

void fs_print_nodos_no_agregados() {
	printf("Nodos no agregados al sistema (addnodo n para agregarlos)\n");
	list_iterate(fs.nodos_no_agregados, (void*) print_nodo);
	printf("\n---------------------------------------------------");
}
/*
 void fs_print_nodos(t_list* nodos) {
 printf("NODOS CONECTADOS AL SISTEMA ('addnodo n' para agregarlos)\n");
 list_iterate(nodos, (void*) print_nodo);
 printf("\n---------------------------------------------------");
 }*/

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
t_nodo_bloque** fs_get_tres_nodo_bloque_libres() {
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

void fs_print_info() {
	printf("INFORMACION ACTUAL DEL FS\n");

	size_t tam = fs_tamanio_bytes(fs);
	size_t tam_libre = fs_tamanio_libre_bytes(fs);
	size_t tam_usado = fs_tamanio_usado_bytes(fs);

	printf("Tamanio : %zd b, %.2f kb, %.2f mb\n", tam, bytes_to_kilobytes(tam),
			bytes_to_megabytes(tam));
	//printf("Tamanio : %.2f kb", bytes_to_kilobytes(fs_tamanio_bytes(fs)));
	//printf("Tamanio : %.2f mb\n", bytes_to_megabytes(fs_tamanio_bytes(fs)));
	//printf("Tamanio : %.2f mb\n", bytes_to_megabytes(fs_tamanio_bytes(fs)));
	//printf("Tamanio : %d MB\n", fs_tamanio_megabytes(fs));
	printf("Tamanio libre: %zd b, %.2f kb, %.2f mb\n", tam_libre,
			bytes_to_kilobytes(tam_libre), bytes_to_megabytes(tam_libre));
	printf("Tamanio usado: %zd b, %.2f kb, %.2f mb\n", tam_usado,
			bytes_to_kilobytes(tam_usado), bytes_to_megabytes(tam_usado));

	//printf("Tamanio libre: %d MB\n", fs_tamanio_libre_megabytes(fs));
	//printf("Tamanio usado: %d MB\n", fs_tamanio_usado_megabytes(fs));

	printf("Cant Nodos conectados: %d\n", list_size(fs.nodos));
	printf("Cant Nodos no agregados: %d\n", list_size(fs.nodos_no_agregados));

	printf("Cant bloques: %d\n", fs_cant_bloques(fs));
	printf("Cant bloques libres: %d\n", fs_cant_bloques_libres(fs));
	printf("Cant bloques usados : %d\n", fs_cant_bloques_usados(fs));

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


void fd_leer_archivos(t_list* archivos) {
	t_archivo* archivo;
	t_bloque_de_datos* bloque_de_datos;
	t_archivo_info* info;
	t_nodo_bloque* nodo_bloque;
	size_t offset = 0;

	char* mapFile = file_get_mapped(FILE_ARCHIVO);
	char* mapBloques = file_get_mapped(FILE_ARCHIVO_BLOQUES);
	int cant_reg = file_get_size(FILE_ARCHIVO) / sizeof(t_archivo_info);

	//comienzo a leer los blquesdel archivo
	int i, j, k;
	for (i = 0; i < cant_reg; i++) {	//una iteracion por archivo
		//creo un nuevo archivo
		archivo = arch_crear();

		//leo la info del archivo, donde me dice tambien cuantos bloques son
		info = malloc(sizeof *info);
		memcpy(info, mapFile + (i * sizeof(t_archivo_info)),
				sizeof(t_archivo_info));

		//lo guardo en el archivo
		archivo->info = info;

		//una vez leido la "cabecera del file voy al archivo de bloques a buscar donde esta cada bloque y sus copias"
		for (j = 0; j < info->cant_bloques; j++) {
			//creo un nuevo bloque de datos que contiene el nro de bloque y donde esta cada copia
			bloque_de_datos = bloque_de_datos_create();

			//lo primero que hay que leer es el nro de bloque
			//memcpy(bloque_de_datos->n_bloque, mapBloques + (sizeof(bloque_de_datos->n_bloque)+ ( j*(BLOQUE_CANT_COPIAS*size_nodo_bloque + sizeof(bloque_de_datos->n_bloque))) ), sizeof(bloque_de_datos->n_bloque));

			//offset += size_bloque_datos;
			memcpy(&bloque_de_datos->n_bloque, mapBloques + offset,	sizeof(bloque_de_datos->n_bloque));

			offset += sizeof(bloque_de_datos->n_bloque);

			//como siempre son BLOQUE_CANT_COPIAS lsizeof(bloque_de_datos->n_bloque)eo eso
			for (k = 0; k < BLOQUE_CANT_COPIAS; k++) {
				//reservo espacio para una copia de donde esta el bloque
				nodo_bloque = malloc(sizeof *nodo_bloque);

				//copio el nodo_bloque del archivo mappeado
				//memcpy(nodo_bloque, mapBloques +(k*size_nodo_bloque+ (j*BLOQUE_CANT_COPIAS*size_nodo_bloque)) , size_nodo_bloque);

				//primero copio el id, porque grabo primero el id

				t_nodo_id_n_bloque nb;
				memcpy(&nb, mapBloques + offset, sizeof(t_nodo_id_n_bloque));

				//ahora tengo que buscar el nodo en la tabla de nodos para guardarlo en memoria
				t_nodo* nodo = NULL;

				//busco si ya existe en la lista de nodos
				if((nodo = fs_buscar_nodo_por_id(nb.nodo_id)) ==NULL){
					//si no existe lo agrego
					t_nodo_base base ;
					//busco en la tabla de nodos del sistema y cargo la info, SIEMPRE VA EXISTIR
					fs_cargar_nodo_base(&base, nb.nodo_id);
					nodo = nodo_new(base.ip, base.puerto, false, base.cant_bloques);
					nodo->base.id = base.id;

					list_add(fs.nodos, (void*)nodo);
				}

				nodo_marcar_bloque_como_usado(nodo, nb.n_bloque);
				nodo_bloque->n_bloque = nb.n_bloque;

				nodo_bloque->nodo = nodo;

				//printf("nodo %d bloques libres %d \n", nodo->base.id, nodo_cant_bloques_libres(nodo));

				//print_nodo(nodo_bloque->nodo);

				offset += sizeof(t_nodo_id_n_bloque);

				//agrego la copia k
				list_add(bloque_de_datos->nodosbloque, (void*) nodo_bloque);

			}
			//agrego el bloque a la lista de bloques de datos del archivo
			list_add(archivo->bloques_de_datos,(t_bloque_de_datos*) bloque_de_datos);
		}

		list_add(archivos, (t_archivo*) archivo);
	}

	file_mmap_free(mapFile, FILE_ARCHIVO);
}

void fd_leer_dirs(t_list* dirs) {
	char* map = file_get_mapped(FILE_DIRECTORIO);
	t_directorio* dir;
	dir = malloc(sizeof *dir);
	int i = 0;
	for (i = 0; i < DIR_CANT_MAX; i++) {

		dir = memcpy(dir, map + (i * sizeof(t_directorio)),
				sizeof(t_directorio));
		if (dir->index != 0) { //si es igual a 0 esta disponible
			list_add(dirs, (void*) dir);
			dir = malloc(sizeof *dir);
		}
	}
	free_null((void*)&dir);
	file_mmap_free(map, FILE_DIRECTORIO);
}

void fs_destroy() {
	list_destroy_and_destroy_elements(fs.nodos, (void*) nodo_destroy);
	list_destroy_and_destroy_elements(fs.nodos_no_agregados, (void*) nodo_destroy);
	list_destroy(fs.directorios);
	list_destroy_and_destroy_elements(fs.archivos, (void*)arch_destroy);
}

#endif /* FILESYSTEM_H_ */
