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

pthread_mutex_t mutex;
bool OPERATIVO = false;
int DIR_ACTUAL = 0;//0 raiz /

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

void nodo_agregar(int id_nodo) {
	fs_agregar_nodo(id_nodo);

	//si no esta operativo verifico si ahora lo esta
	if(!OPERATIVO){
		OPERATIVO = fs_esta_operativo();
	}
}

void print_directorio_actual(){
	char* dir_actual = fs_dir_get_path(DIR_ACTUAL);
	printf("Dir Actual: %s - ", dir_actual);
	free(dir_actual);
	dir_actual = NULL;
}

void iniciar_consola() {
	int nodo_id;
	//int dir_id, dir_padre;
	char* archivo_nombre;
	char* dir_nombre;
	int nro_bloque;
	//char* buff  ;
	char comando[COMMAND_MAX_SIZE];

	printf("INICIO CONSOLA\n");

	bool fin = false;
	while (!fin) {
		printf("\nINGRESAR COMANDO: \n");
		print_directorio_actual();

		leer_comando_consola(comando);

		//separo todoo en espacios ej: [copiar, archivo1, directorio0]
		char** input_user = separar_por_espacios(comando);
		e_comando cmd = getComando(input_user[0]);

		if (cmd != NODO_AGREGAR && cmd!=SALIR && !OPERATIVO) {
			if(!OPERATIVO){
				fs_print_no_operativo();
				fs_print_nodos_no_agregados();
			}
		} else {
			switch (cmd) {
			case ARCHIVO_VERBLOQUE:	//filevb nombre dir nro_bloque
				archivo_nombre = input_user[1];
				nro_bloque = atoi(input_user[3]);
				pthread_mutex_lock(&mutex);
				archivo_ver_bloque(archivo_nombre, nro_bloque);
				pthread_mutex_unlock(&mutex);
				break;
			case ARCHIVO_LISTAR:	//lsfile
				pthread_mutex_lock(&mutex);
				fs_print_archivos();
				pthread_mutex_unlock(&mutex);
				break;
			case ARCHIVO_INFO:	//info
				//ej:fileinfo
				archivo_nombre = input_user[1];
				pthread_mutex_lock(&mutex);
				archivo_info(archivo_nombre);
				pthread_mutex_unlock(&mutex);
				break;
			case NODO_AGREGAR:			//addnodo 1
				//printf("comando ingresado: agregar nodo\n");
				nodo_id = atoi(input_user[1]);
				pthread_mutex_lock(&mutex);
				nodo_agregar(nodo_id);
				pthread_mutex_unlock(&mutex);
				break;
			case ARCHIVO_COPIAR_MDFS_A_LOCAL:
				//ej: copytolocal 3registros.txt 0 /home/utnso/Escritorio/
				archivo_nombre = input_user[1];
				dir_nombre = input_user[3];
				pthread_mutex_lock(&mutex);
				archivo_copiar_mdfs_a_local(archivo_nombre, dir_nombre);
				pthread_mutex_unlock(&mutex);
				break;
			case ARCHIVO_COPIAR_LOCAL_A_MDFS:
				//ejemplo: copy /home/utnso/Escritorio/uno.txt 1
				//donde 1 es un directorio existente en el fs, 0 es el raiz
				archivo_nombre = input_user[1];

				archivo_copiar_local_a_mdfs(archivo_nombre);

				break;
			case NODO_LISTAR_NO_AGREGADOS:			//lsnodop
				//printf("listar nodos no agregados al fs, falta hacerle un addNodo\n");
				pthread_mutex_lock(&mutex);
				fs_print_nodos_no_agregados();
				pthread_mutex_unlock(&mutex);
				break;
			case NODO_ELIMINAR:
				//printf("comando ingresado: elimnar nodo\n");
				nodo_id = atoi(input_user[1]);
				pthread_mutex_lock(&mutex);
				nodo_eliminar(nodo_id);
				pthread_mutex_unlock(&mutex);
				break;
			case CAMBIAR_DIRECTORIO: //changedir ej. cd /home
				pthread_mutex_lock(&mutex);
				cambiar_directorio(input_user[1]);
				pthread_mutex_unlock(&mutex);
				break;
			case DIRECTORIO_CREAR:			//ej: mkdir carpetauno 0
				dir_nombre = input_user[1];			//nombre
				pthread_mutex_lock(&mutex);
				directorio_crear(dir_nombre);
				pthread_mutex_unlock(&mutex);
				break;
			case DIRECTORIO_RENOMBRAR://renamedir hola holados
				pthread_mutex_lock(&mutex);
				directorio_renombrar(input_user[1], input_user[2]);
				pthread_mutex_unlock(&mutex);
				break;
			case DIRECTORIO_ELIMINAR:
				dir_nombre = input_user[1];
				pthread_mutex_lock(&mutex);
				directorio_eliminar(dir_nombre);
				pthread_mutex_unlock(&mutex);
				break;
			case DIRECTORIO_LISTAR:			//lsdir
				pthread_mutex_lock(&mutex);
				fs_print_dirs();
				pthread_mutex_unlock(&mutex);
				break;
			case FORMATEAR:			//format
				pthread_mutex_lock(&mutex);
				fs_formatear();
				pthread_mutex_unlock(&mutex);
				break;
			case FS_INFO:			//info
				pthread_mutex_lock(&mutex);
				fs_print_info();
				pthread_mutex_unlock(&mutex);
				break;
			case SALIR:			//exit
				pthread_mutex_lock(&mutex);
				fs_desconectarse();
				pthread_mutex_unlock(&mutex);
				fin = true;
				break;
			default:
				printf("Comando desconocido\n");
				break;
			}
		}
		free_split(input_user);
	}
}

void directorio_renombrar(char* nombre, char* nuevo_nombre){
	if (!fs_existe_dir(nombre, DIR_ACTUAL)) {
		printf("El directorio %s no existe\n", nombre);
		return;
	}
	int id = fs_buscar_directorio_id_por_nombre(nombre, DIR_ACTUAL);

	//verifico si ya existe el dir con nuevo_nombre
	if(fs_existe_dir(nuevo_nombre, DIR_ACTUAL)){
		printf("EL directorio %s ya existe\n", nuevo_nombre);
		return;
	}
	////////////////////////////////////////////
	fs_dir_renombrar(id, nuevo_nombre);

}

void directorio_eliminar(char* nombre){

	if (!fs_existe_dir(nombre, DIR_ACTUAL)) {
		printf("El directorio no existe\n");
		return;
	}
	int id = fs_buscar_directorio_id_por_nombre(nombre, DIR_ACTUAL);

	if(!fs_dir_esta_vacio(id)){
		printf("El directorio no esta vacio\n");
		return ;
	}
	//borro tod0!
	fs_dir_eliminar_por_id(id);

	printf("Eliminado!\n");
}

void directorio_crear(char* nombre){
	if (!fs_existe_dir(nombre, DIR_ACTUAL)) {
		dir_crear(fs.directorios, nombre, DIR_ACTUAL);
		printf("El directorio se creo.\n");
	} else
		printf("El directorio ya existe. lsdir para ver los dirs creados\n");
}


void nodo_eliminar(int nodo_id){
	//elimino el nodo y vuelve a la lista de nodos_no_conectados
	fs_eliminar_nodo(nodo_id);

	printf(	"el nodo %d  se ha eliminado del fs. Paso a la lista de nodos no agregados\n",	nodo_id);
	fs_print_nodos_no_agregados();
}
void archivo_copiar_local_a_mdfs(char*file_local){
	pthread_mutex_lock(&mutex);
	if (file_exists(file_local)) {
		//verifico que exista en archivo en el fs y dentro de la carpeta
		if (!fs_existe_archivo(file_local, DIR_ACTUAL)) {
			fs_copiar_archivo_local_al_fs(file_local, DIR_ACTUAL);
		} else {
			printf(	"el archivo [%s] con dir:%d YA EXISTE en el mdfs. lsfiles para ver los archivos \n",file_local, DIR_ACTUAL);
		}
	} else
		printf("el archvo no existe: %s\n", file_local);
	pthread_mutex_unlock(&mutex);
}

void cambiar_directorio(char* path){
	int dir_id;
	if (strcmp(path, "..") == 0) { //si ingreso cd ..
		DIR_ACTUAL = fs_dir_get_padre(DIR_ACTUAL);
	} else {
		dir_id = fs_dir_get_index(path, DIR_ACTUAL);
		if (dir_id >= 0) {
			//DIR_ANTERIOR = DIR_ACTUAL;
			DIR_ACTUAL = dir_id;
		} else {
			printf("Directorio %d no existente\n", dir_id);
		}
	}
}

void archivo_copiar_mdfs_a_local(char* nombre, char* destino){

	//verifico que exista el archivo en el mdfs
	if (fs_existe_archivo(nombre, DIR_ACTUAL)) {
		fs_copiar_mdfs_a_local(nombre, DIR_ACTUAL, destino);
	} else
		printf("EL archivo no existe en el mdfs: %s\n",	nombre);
}

void archivo_info(char* nombre){

	if (fs_existe_archivo(nombre, DIR_ACTUAL))
		fs_print_archivo(nombre, DIR_ACTUAL);
	else
		printf("el archivo no existe: %s\n", nombre);

}

void archivo_ver_bloque(char* nombre, int nro_bloque){
	if (fs_existe_archivo(nombre, DIR_ACTUAL))

		if (fs_archivo_ver_bloque(nombre, DIR_ACTUAL, nro_bloque) < 0) {
			log_info(logger,
					"No se pudo obtener el bloque %d del archivo %s dir %d",
					nro_bloque, nombre, DIR_ACTUAL);
		} else {
			log_info(logger, "ver bloque nro %d en archivo %s en dir %d ",
					nro_bloque, nombre, DIR_ACTUAL);
		}
	else
		printf("el archivo no existe: %s\n", nombre);
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

	pthread_mutex_init(&mutex, NULL);
	//comento para que siempre que ejecute tome el mismo cwd
	//set_cwd();

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
	int id_nodo;

	switch (msg->header.id) {
	case NODO_CONECTAR_CON_FS: //primer mensaje del nodo
		destroy_message(msg);
		msg = string_message(FS_NODO_QUIEN_SOS, "", 0);
		enviar_mensaje(fd, msg);
		destroy_message(msg);

		msg = recibir_mensaje(fd);
		//print_msg(msg);

		t_nodo* nodo ;
		pthread_mutex_lock(&mutex);
		if(fs_existe_nodo_por_ip_puerto(msg->stream, msg->argv[0])){
			//si ya existe lo busco
			nodo = fs_buscar_nodo_por_ip_puerto(msg->stream, msg->argv[0]);
			//si existe lo borro y lo paso a la lista de nodos_no_agregados
			fs_eliminar_nodo(nodo->base.id);
		}
		else{
			nodo = nodo_new(msg->stream,  msg->argv[0],(bool) msg->argv[1], msg->argv[2]); //0 puerto, 1 si es nuevo o no, 2 es la cant bloques
			id_nodo = fs_get_nodo_id_en_archivo_nodos(msg->stream, msg->argv[0]);
			//tengo que ver si ya existe la lista de nodos del fs
			nodo->base.id =id_nodo;
			//agrego el nodo a la lista de nodos nuevos
			list_add(fs.nodos_no_agregados, (void*) nodo);
		}

		printf("Se conecto el nodo %d,  %s:%d | %s\n", nodo->base.id,nodo->base.ip, nodo->base.puerto, nodo_isNew(nodo));
		log_info(logger, "Se conecto el nodo %d,  %s:%d | %s", nodo->base.id,nodo->base.ip, nodo->base.puerto, nodo_isNew(nodo));

		//ESTO NO VA PERO LO AGREGO PARA NO TENER QUE ESTAR AGREGANDO EL NODO CADA VEZ QUE LEVANTO EL FS
		nodo_agregar(nodo->base.id);

		pthread_mutex_unlock(&mutex);

		break;
	default:
		printf("mensaje desconocido\n");
		break;
	}
	destroy_message(msg);
}

