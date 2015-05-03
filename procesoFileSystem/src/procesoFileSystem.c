/*
 ============================================================================
 Name        : procesoFileSystem.c
 Author      : 
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */
#include <libgen.h>
#include <stdio.h>
#include <stdlib.h>
#include <commons/log.h>
#include <commons/config.h>
#include <commons/collections/list.h>
#include <pthread.h>
#include "consola.h"
#include "fileSystem.h"

//#include <util.h>



#define FILE_CONFIG "/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoFileSystem/config.txt"
#define FILE_LOG "/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoFileSystem/log.txt"

t_log* logger;
t_config* config;
t_fileSystem fs;

void iniciar_consola();
void nuevosNodos();
void procesar_mensaje_nodo(int i, t_msg* msg);
void inicializar();
void finalizar();
void agregar_nodo_al_fs(int id);
void iniciar_server_nodos_nuevos();

int main(void) {

	inicializar();

	iniciar_server_nodos_nuevos();

	iniciar_consola();

	finalizar();
	return EXIT_SUCCESS;
}

void iniciar_server_nodos_nuevos() {
	/*
	 * ABRO UN THREAD EL SERVER PARA QUE SE CONECTEN NODOS
	 */
	pthread_t th_nuevosNodos;
	pthread_attr_t tattr;

	pthread_attr_init(&tattr);
	pthread_attr_setdetachstate(&tattr,PTHREAD_CREATE_DETACHED);
	pthread_create(&th_nuevosNodos, &tattr, (void*)nuevosNodos, NULL);
	pthread_attr_destroy(&tattr);
	//espera a que termine el nodo: bloquea
	//pthread_join(th_nuevosNodos, NULL);
	//pthread_detach(th_nuevosNodos);
}

void agregar_nodo_al_fs(int id_nodo) {

	fs_agregar_nodo(&fs, id_nodo);

	t_nodo* nodo = fs_buscar_nodo_por_id(&fs, id_nodo);
	log_info(logger, "El nodo %d fue agregado al fs. %s:%d", id_nodo, nodo->ip, nodo->puerto);
}


void iniciar_consola() {
	int nodo_id;
	int dir_id;
	char* archivo_nombre;
	char* dir_nombre;
	//char* buff  ;


	char comando[COMMAND_MAX_SIZE];
	printf("INICIO CONSOLA\n");

	bool fin = false;
	while (!fin) {
		printf("INGRESAR COMANDO: ");

		leer_comando_consola(comando);

		//separo todo en espacios ej: [copiar, archivo1, directorio0]
		char** input_user = separar_por_espacios(comando);

		switch (getComando(input_user[0])) {
		case ARCHIVO_LISTAR:
			fs_print_archivos(&fs);
			break;
		case ARCHIVO_INFO://info
			//ej:fileinfo
			archivo_nombre = input_user[1];
			dir_id = atoi(input_user[2]);
			//dir_id = 0;
			//primer_param_char =  "/home/utnso/Escritorio/3registros.txt";

			//printf("%s", basename(archivo_nombre));

			if(fs_existe_archivo(&fs, archivo_nombre, dir_id))
				fs_print_archivo(&fs, archivo_nombre, dir_id);
			else
				printf("el archivo no existe: %s", archivo_nombre);

			break;
		case NODO_AGREGAR:
			printf("comando ingresado: agregar nodo\n");
			nodo_id = atoi(input_user[1]);

			agregar_nodo_al_fs(nodo_id);

			break;

		case ARCHIVO_COPIAR_MDFS_A_LOCAL:
			//archivo_nombre = input_user[1];
			archivo_nombre = input_user[1];
			archivo_nombre = "/home/utnso/Escritorio/3registros.txt";
			dir_id = atoi(input_user[2]);

			if(fs_existe_archivo(&fs, archivo_nombre, dir_id)){
				fs_exportar_a_fs_local(&fs, archivo_nombre);
			}
			else
				printf("EL archivo no existe en el mdfs: %s\n", archivo_nombre);

			break;
		case ARCHIVO_COPIAR_LOCAL_A_MDFS:
			//ejemplo: copy /home/utnso/Escritorio/uno.txt 1
			//donde 1 es un directorio existente en el fs, 0 es el raiz


			archivo_nombre = input_user[1];

			//char* archivo_local = "/home/utnso/Escritorio/3registros.txt";
			//char* archivo_local = "/home/utnso/Escritorio/dos.txt";
			//segundo_param_int = input_user[2];//es el directorio donde se va copiar
			//dir_id = 0;//directorio raiz
			dir_id = atoi(input_user[2]);

			if(file_exists(archivo_nombre)){
				//verifico que exista en archivo en el fs y dentro de la carpeta
				if(!fs_existe_archivo(&fs, archivo_nombre, dir_id)){
					fs_copiar_archivo_local_al_fs(&fs, archivo_nombre, dir_id);
					printf("archivo copiado y agregado al fs----------------------------\n");
				}
				else{
					printf("el archivo [%s] con dir:%d YA EXISTE en el mdfs. lsfiles para ver los archivos \n", archivo_nombre, dir_id);
				}




			}
			else
				printf("el archvo no existe: %s", archivo_nombre);

			//free_null(archivo_nombre);



			break;
		case NODO_LISTAR_NO_AGREGADOS://lsnodop
			//printf("listar nodos no agregados al fs, falta hacerle un addNodo\n");
			fs_print_nodos(fs.nodos_no_agregados);

			break;

		case NODO_ELIMINAR:
			//printf("comando ingresado: elimnar nodo\n");
			nodo_id = atoi(input_user[1]);
			//elimino el nodo y vuelve a la lista de nodos_no_conectados
			fs_eliminar_nodo(&fs, nodo_id);

			printf("el nodo %d  se ha eliminado del fs. Paso a la lista de nodos no agregados\n", nodo_id);
			fs_print_nodos(fs.nodos_no_agregados);
			break;
		case DIRECTORIO_CREAR://ej: mkdir carpetauno 0
			dir_nombre = input_user[1];//nombre
			//segundo_param_int = atoi(input_user[2]);//padre
			dir_id = atoi(input_user[2]);//el padre

			if(!fs_existe_dir(&fs, dir_id)){
				dir_crear(fs.directorios, dir_nombre, dir_id);
				printf("El directorio se creo.\n");
			}
			else
				printf("el directorio ya existe. lsdir para ver los dirs creados\n");


			break;
		case DIRECTORIO_LISTAR://lsdir
			fs_print_dirs(&fs);

			break;
		case FORMATEAR://format
			fs_formatear(&fs);
			break;
		case FS_INFO://info
			//printf("Mostrar info actual del fs del fileSystem\n");
			fs_print_info(&fs);

			break;
		case SALIR://salir
			printf("comando ingresado: salir\n");

			fs_desconectarse(&fs);

			fin = true;
			break;
		default:
			printf("comando desconocido\n");
			break;
		}

		//free(*input_user);
		int i=0;
		while(input_user[i]!=NULL){
			free_null(input_user[i]);
			i++;
		}
		free_null(input_user);
	}
}

void finalizar() {

	fs_destroy(&fs);

	log_destroy(logger);
	config_destroy(config);
	printf("bb world!!!!\n");

}

void inicializar() {
	logger = log_create(FILE_LOG, "FileSystem", true, LOG_LEVEL_INFO);
	config = config_create(FILE_CONFIG);

	fs_create(&fs);

	//para guardar los nodos recien conectados
	//nodos_nuevos = list_create();

}

void nuevosNodos() {
	server_socket_select(config_get_int_value(config, "PUERTO_LISTEN"), (void*)procesar_mensaje_nodo);
}

void procesar_mensaje_nodo(int i, t_msg* msg) {
	//leer el msg recibido
	//print_msg(msg);

	switch (msg->header.id) {
	case NODO_CONECTAR_CON_FS: //primer mensaje del nodo
		destroy_message(msg);
		msg = string_message(FS_NODO_QUIEN_SOS, "", 0);
		enviar_mensaje(i, msg);
		destroy_message(msg);

		msg = recibir_mensaje(i);
		//print_msg(msg);
		t_nodo* nodo = nodo_new(msg->stream, (uint16_t) msg->argv[0],
				(bool) msg->argv[1], msg->argv[2]); //0 puerto, 1 si es nuevo o no, 2 es la cant bloques



		//le asigno un nombre
		nodo->id = get_id_nodo_nuevo();

		//print_nodo(nodo);

		//agrego el nodo a la lista de nodos nuevos
		list_add(fs.nodos_no_agregados, (void*) nodo);

		log_info(logger, "se conecto el nodo %d,  %s:%d | %s",
				nodo->id, nodo->ip, nodo->puerto, nodo_isNew(nodo));

		agregar_nodo_al_fs(nodo->id);

		break;
	default:
		printf("mensaje desconocido\n");
		break;
	}
	destroy_message(msg);
}

