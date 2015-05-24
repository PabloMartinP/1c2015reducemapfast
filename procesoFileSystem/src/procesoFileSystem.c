/*
 ============================================================================
 Name        : procesoFileSystem.c
 Author      : 
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include "procesoFileSystem.h"

bool OPERATIVO = false;

int main(void) {

	inicializar();

	iniciar_server_nodos_nuevos();

	iniciar_consola();

	finalizar();
	return EXIT_SUCCESS;
}

void iniciar_server_nodos_nuevos() {

	void _nodos_nuevos(){

		if(server_socket_select(config_get_int_value(config, "PUERTO_LISTEN"),	(void*) procesar_mensaje_nodo)<0){
			log_error(logger, "No se pudo iniciar el server de nodos nodos en el puerto %d", config_get_int_value(config, "PUERTO_LISTEN"));
			exit(EXIT_FAILURE);
		}
	}

	/*
	 * ABRO UN THREAD EL SERVER PARA QUE SE CONECTEN NODOS
	 */
	pthread_t th_nuevosNodos;
	pthread_attr_t tattr;

	pthread_attr_init(&tattr);
	pthread_attr_setdetachstate(&tattr, PTHREAD_CREATE_DETACHED);
	pthread_create(&th_nuevosNodos, &tattr, (void*) _nodos_nuevos, NULL);
	pthread_attr_destroy(&tattr);
	//espera a que termine el nodo: bloquea
	//pthread_join(th_nuevosNodos, NULL);
	//pthread_detach(th_nuevosNodos);
}

void agregar_nodo_al_fs(int id_nodo) {
	fs_agregar_nodo(id_nodo);

	//si no esta operativo verifico si ahora lo esta
	if(!OPERATIVO){
		OPERATIVO = fs_esta_operativo();
	}
}

void iniciar_consola() {
	//int i;
	int nodo_id;
	int dir_id;
	char* archivo_nombre;
	char* dir_nombre;
	int nro_bloque;
	//char* buff  ;
	char comando[COMMAND_MAX_SIZE];

	printf("INICIO CONSOLA\n");

	bool fin = false;
	while (!fin) {

		printf("\nINGRESAR COMANDO: ");

		leer_comando_consola(comando);

		//separo todo en espacios ej: [copiar, archivo1, directorio0]
		char** input_user = separar_por_espacios(comando);
		e_comando cmd = getComando(input_user[0]);

		if (cmd != NODO_AGREGAR && cmd!=SALIR) {
			if(!OPERATIVO){
				printf("FS no operativo. \n");
				printf("Cantidad minima de nodos conectados: %d\n", fs.cant_nodos_minima);
				printf("Cantidad de nodos conectados: %d\n", list_size(fs.nodos));
				printf("Cantidad de nodos no conectados: %d\n", list_size(fs.nodos_no_agregados));
				fs_print_nodos_no_agregados();
				//OPERATIVO = false;
			}else{
				//OPERATIVO = true;
			}
		} else {
			switch (cmd) {
			case ARCHIVO_VERBLOQUE:	//filevb nombre dir nro_bloque
				archivo_nombre = input_user[1];
				dir_id = atoi(input_user[2]);
				nro_bloque = atoi(input_user[3]);

				if (fs_existe_archivo(archivo_nombre, dir_id))

					if(fs_archivo_ver_bloque(archivo_nombre, dir_id, nro_bloque)<0){
						log_info(logger, "No se pudo obtener el bloque %d del archivo %s dir %d", nro_bloque, archivo_nombre, dir_id);
					}
					else{
						log_info(logger, "ver bloque nro %d en archivo %s en dir %d ", nro_bloque, archivo_nombre, dir_id);
					}

				else
					printf("el archivo no existe: %s\n", archivo_nombre);
				break;
			case ARCHIVO_LISTAR:	//lsfile
				fs_print_archivos();
				break;
			case ARCHIVO_INFO:	//info
				//ej:fileinfo
				archivo_nombre = input_user[1];
				dir_id = atoi(input_user[2]);
				//dir_id = 0;

				if (fs_existe_archivo(archivo_nombre, dir_id))
					fs_print_archivo(archivo_nombre, dir_id);
				else
					printf("el archivo no existe: %s\n", archivo_nombre);

				break;
			case NODO_AGREGAR:			//addnodo 1
				//printf("comando ingresado: agregar nodo\n");
				nodo_id = atoi(input_user[1]);
				fs_agregar_nodo(nodo_id );

				break;

			case ARCHIVO_COPIAR_MDFS_A_LOCAL:
				//ej: copytolocal 3registros.txt 0 /home/utnso/Escritorio/
				archivo_nombre = input_user[1];
				//archivo_nombre = "/home/utnso/Escritorio/3registros.txt";
				dir_id = atoi(input_user[2]);
				dir_nombre = input_user[3];

				//verifico que exista el archivo en el mdfs
				if (fs_existe_archivo(archivo_nombre, dir_id)) {
					fs_copiar_mdfs_a_local(archivo_nombre, dir_id, dir_nombre);
				} else
					printf("EL archivo no existe en el mdfs: %s\n",	archivo_nombre);

				break;
			case ARCHIVO_COPIAR_LOCAL_A_MDFS:
				//ejemplo: copy /home/utnso/Escritorio/uno.txt 1
				//donde 1 es un directorio existente en el fs, 0 es el raiz

				archivo_nombre = input_user[1];

				//dir_id = 0;//directorio raiz
				dir_id = atoi(input_user[2]);

				if (file_exists(archivo_nombre)) {
					//verifico que exista en archivo en el fs y dentro de la carpeta
					if (!fs_existe_archivo(archivo_nombre, dir_id)) {
						if(fs_copiar_archivo_local_al_fs(archivo_nombre, dir_id)<0){
							printf("No se pudo copiar el archivo\n");
						}else
							printf("archivo copiado y agregado al fs----------------------------\n");
					} else {
						printf("el archivo [%s] con dir:%d YA EXISTE en el mdfs. lsfiles para ver los archivos \n",	archivo_nombre, dir_id);
					}
				} else
					printf("el archvo no existe: %s\n", archivo_nombre);

				break;
			case NODO_LISTAR_NO_AGREGADOS:			//lsnodop
				//printf("listar nodos no agregados al fs, falta hacerle un addNodo\n");
				fs_print_nodos_no_agregados();

				break;

			case NODO_ELIMINAR:
				//printf("comando ingresado: elimnar nodo\n");
				nodo_id = atoi(input_user[1]);
				//elimino el nodo y vuelve a la lista de nodos_no_conectados
				fs_eliminar_nodo(nodo_id);

				printf("el nodo %d  se ha eliminado del fs. Paso a la lista de nodos no agregados\n", nodo_id);
				fs_print_nodos_no_agregados();
				break;
			case DIRECTORIO_CREAR:			//ej: mkdir carpetauno 0
				dir_nombre = input_user[1];			//nombre
				//segundo_param_int = atoi(input_user[2]);//padre
				dir_id = atoi(input_user[2]);			//el padre

				if (!fs_existe_dir(dir_id)) {
					dir_crear(fs.directorios, dir_nombre, dir_id);
					printf("El directorio se creo.\n");
				} else
					printf("el directorio ya existe. lsdir para ver los dirs creados\n");

				break;
			case DIRECTORIO_LISTAR:			//lsdir
				fs_print_dirs();

				break;
			case FORMATEAR:			//format
				fs_formatear();
				break;
			case FS_INFO:			//info
				//printf("Mostrar info actual del fs del fileSystem\n");
				fs_print_info();

				break;
			case SALIR:			//exit
				printf("comando ingresado: salir\n");

				fs_desconectarse();

				fin = true;
				break;
			default:
				printf("comando desconocido\n");
				break;
			}
		}

		//free(*input_user);
		int i = 0;
		while (input_user[i] != NULL) {
			free_null((void*)&input_user[i]);
			i++;
		}
		free_null((void*)&input_user);
	}
}

void finalizar() {

	fs_destroy();

	log_destroy(logger);
	config_destroy(config);
	printf("FS Terminado!!!!\n");
}

void set_cwd(){

	char*aux;
	aux = convertir_path_absoluto(FILE_LOG);
	strcpy(FILE_LOG, aux);
	free(aux);
	aux = convertir_path_absoluto(FILE_CONFIG);
	strcpy(FILE_CONFIG, aux);
	free(aux);
	aux = convertir_path_absoluto(FILE_ARCHIVO);
	strcpy(FILE_ARCHIVO, aux);
	free(aux);

	aux = convertir_path_absoluto(FILE_ARCHIVO_BLOQUES);
	strcpy(FILE_ARCHIVO_BLOQUES, aux);
	free(aux);

	aux = convertir_path_absoluto(FILE_DIRECTORIO);
	strcpy(FILE_DIRECTORIO, aux);
	free(aux);

	aux = convertir_path_absoluto(FILE_NODOS);
	strcpy(FILE_NODOS, aux);
	free(aux);
}
void inicializar() {
	int rs;

	//comento para que siempre que ejecute tome el mismo cwd
	set_cwd();

	//inicializo el log

	logger = log_create(FILE_LOG, "FileSystem", true, LOG_LEVEL_INFO);

	//inicializo el config

	if (!file_exists(FILE_CONFIG)) {
		log_info(logger, "No existe el archivo config - %s", FILE_CONFIG);
		handle_error("config");
	}

	config = config_create(FILE_CONFIG);


	//INICIO EL FS
	if((rs = fs_create())<0){
		log_info(logger, "No se pudo crear el fs");
		handle_error("config");
		//return -1;
	}

	//le cargo los nodos necesarios para que valide si puede estar operativo
	fs.cant_nodos_minima = config_get_int_value(config, "CANT_NODOS_MINIMA");

}


void procesar_mensaje_nodo(int fd, t_msg* msg) {
	//leer el msg recibido
	//print_msg(msg);

	switch (msg->header.id) {
	case NODO_CONECTAR_CON_FS: //primer mensaje del nodo
		destroy_message(msg);
		msg = string_message(FS_NODO_QUIEN_SOS, "", 0);
		enviar_mensaje(fd, msg);
		destroy_message(msg);

		msg = recibir_mensaje(fd);
		//print_msg(msg);
		t_nodo* nodo = nodo_new(msg->stream, (uint16_t) msg->argv[0],(bool) msg->argv[1], msg->argv[2]); //0 puerto, 1 si es nuevo o no, 2 es la cant bloques

		//tengo que ver si ya existe la lista de nodos del fs
		nodo->base.id = fs_get_nodo_id_en_archivo_nodos(msg->stream, msg->argv[0]);

		//print_nodo(nodo);

		//agrego el nodo a la lista de nodos nuevos
		list_add(fs.nodos_no_agregados, (void*) nodo);

		printf("Se conecto el nodo %d,  %s:%d | %s\n", nodo->base.id,nodo->base.ip, nodo->base.puerto, nodo_isNew(nodo));
		log_info(logger, "Se conecto el nodo %d,  %s:%d | %s", nodo->base.id,nodo->base.ip, nodo->base.puerto, nodo_isNew(nodo));

		//ESTO NO VA PERO LO AGREGO PARA NO TENER QUE ESTAR AGREGANDO EL NODO CADA VEZ QUE LEVANTO EL FS
		//agregar_nodo_al_fs(nodo->id);

		//agregar_nodo_al_fs(nodo->base.id);


		break;
	default:
		printf("mensaje desconocido\n");
		break;
	}
	destroy_message(msg);
}

