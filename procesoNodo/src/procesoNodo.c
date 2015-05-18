#include <stdio.h>
#include <stdlib.h>
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
//#include "socket.h"

typedef struct{
	char ip[15];
	int puerto;
	char archivo[255];
}t_files_reduce;

char FILE_CONFIG [1024] = "/config.txt";
char FILE_LOG [1024] = "/log.txt";

/* since pipes are unidirectional, we need two pipes.
   one for data to flow from parent's stdout to child's
   stdin and the other for child's stdout to flow to
   parent's stdin */

#define NUM_PIPES          2

#define PARENT_WRITE_PIPE  0
#define PARENT_READ_PIPE   1

int pipes[NUM_PIPES][2];
//int pipes_sort[NUM_PIPES][2];

/* always in a pipe[], pipe[0] is for read and
   pipe[1] is for write */
#define READ_FD  0
#define WRITE_FD 1

#define PARENT_READ_FD  ( pipes[PARENT_READ_PIPE][READ_FD]   )
#define PARENT_WRITE_FD ( pipes[PARENT_WRITE_PIPE][WRITE_FD] )

#define CHILD_READ_FD   ( pipes[PARENT_WRITE_PIPE][READ_FD]  )
#define CHILD_WRITE_FD  ( pipes[PARENT_READ_PIPE][WRITE_FD]  )

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
void procesar_mensaje(int fd, t_msg* msg);
void incicar_server();
void agregar_cwd(char* file);
void* ejecutar_script(char* script, void* (*procesar_std)()) ;
int ordenar_y_guardar_en_temp(char* file_desordenado, char* destino);
int aplicar_reduce(t_list* files_maped, char* script_reduce, char* filename_result);
int get_index_menor(char** keys, int cant);
bool alguna_key_distinta_null(char** keys, int cant);
int aplicar_reduce_local(t_list* files, char*script_reduce, char* filename_result);
char* getFileContent_another_node(t_files_reduce* fr);
char* convertir_a_temp_path_filename(char* filename);
//char* ejecutar_script_sort(char* filename);
/*
 * graba el temp concatenandole el timenow en el filename para que sea unico
 */
int grabar_en_temp(char* filename, char* data);

/*
 * devuelve el archivo creado en el  temp del nodo
 */
int aplicar_map(int n_bloque, char* script_map, char* filename_result);
/*
 * main
 */
int TAMANIO_DATA;
int CANT_BLOQUES;
int main(int argc, char *argv[]) {
	//por param le tiene que llegar el tamaño del archivo data.bin
	//por ahora hardcodeo 100mb, serian 10 bloques de 20 mb


	inicializar();

	probar_conexion_fs();

	//inicio el server para atender las peticiones del fs
	iniciar_server_thread();


	//test map
	char* timenow = temporal_get_string_time();

	char* file_map1 = string_new();
	string_append(&file_map1, "job_map_");
	string_append(&file_map1, timenow);
	string_append(&file_map1, "_0.txt");
	aplicar_map(0, "/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoJob/map.sh", file_map1);
	printf("%s\n", file_map1);

	char* file_map2 = string_new();
	string_append(&file_map2, "job_map_");
	string_append(&file_map2, timenow);
	string_append(&file_map2, "_1.txt");
	aplicar_map(1, "/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoJob/map.sh", file_map2);
	printf("%s\n", file_map2);
//////////////////////////////////////////////////////////////////
	//test reduce local
	char* file_reduce_result = string_new();
	string_append(&file_reduce_result, "job_reduced_result_");
	string_append(&file_reduce_result, timenow);
	t_list* files = list_create();
	list_add(files, (void*)file_map1);
	list_add(files, (void*)file_map2);
	aplicar_reduce_local(files, "/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoJob/reduce.sh", file_reduce_result);
	list_destroy(files);

//////////////////////////////////////////////////////////////////////////
	/*
	//test reduce de los dos archivos mapeados y en otro nodo?
	//creo un archivo para reducir
	t_list* files_to_reduce = list_create();
	t_files_reduce* file_reduce1 = malloc(sizeof *file_reduce1);
	strcpy(file_reduce1->archivo, file_map1);
	strcpy(file_reduce1->ip, NODO_IP());
	file_reduce1->puerto = NODO_PORT();
	list_add(files_to_reduce, file_reduce1);

	//creo otro archivo para reducir
	t_files_reduce* file_reduce2 = malloc(sizeof *file_reduce2);
	strcpy(file_reduce2->archivo, file_map2);
	strcpy(file_reduce2->ip, NODO_IP());
	file_reduce2->puerto = NODO_PORT();
	list_add(files_to_reduce, file_reduce2);

	char* file_reduce_result = string_new();
	string_append(&file_reduce_result, "job_reduced_result_");
	string_append(&file_reduce_result, timenow);
	aplicar_reduce(files_to_reduce, "/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoJob/reduce.sh", file_reduce_result);
	printf("%s\n", file_reduce_result);
	free(file_map1);
	free(file_map2);
	list_destroy(files_to_reduce);
	free(file_reduce1);
	free(file_reduce2);
	free(file_reduce_result);
*/
	free_null((void*) &timenow);

	//bool fin = true	;
	while (!FIN);
	finalizar();

	return EXIT_SUCCESS;
}

char* getFileContent_another_node(t_files_reduce* fr){
	int fd;
	int rs;

	//conecto con el nodo
	if((fd = client_socket(fr->ip, fr->puerto))<0){
		printf("No se pudo conectar el nodo. %s:%d\n", fr->ip, fr->puerto);
		perror("client_socket");
		return NULL;
	}

	//le pido que me mande el archivo que tiene almacenado en tmp
	t_msg* msg = NULL;
	msg = string_message(NODO_GET_FILECONTENT, fr->archivo, 0);
	if((rs = enviar_mensaje(fd, msg))<0){
		printf("No se pudo enviar el mensaje al nodo. %s:%d\n", fr->ip, fr->puerto);
		perror("enviar_mensaje:NODO_GET_FILECONTENT ");
		return NULL;
	}
	destroy_message(msg);
	//recibo el archivo
	if((msg = recibir_mensaje(fd))==NULL){
		printf("No se pudo recibir el mensaje nodo. %s:%d\n", fr->ip, fr->puerto);
		perror("recibir_mensaje:NODO_GET_FILECONTENT ");
		return NULL;
	}

	char* data =NULL;
	memcpy(data, msg->stream, strlen(msg->stream)+1);
	destroy_message(msg);
	return data;
}

int aplicar_reduce_local(t_list* files, char*script_reduce, char* filename_result){
	int i=0;
	int rs;
	int cant_files = list_size(files);
	t_list* files_abs = list_create();
	void _convertir_a_absoluto(char* filename){
		list_add(files_abs, convertir_a_temp_path_filename(filename));
	}
	list_iterate(files, (void*)_convertir_a_absoluto);
	//creo un descriptor de archivo por cada file
	FILE** fd = malloc(cant_files* sizeof(FILE*));

	//abro los archivos
	i=0;
	void _open_file(char* filename){
		fd[i] = fopen(filename, "r");
		i++;
	}
	list_iterate(files_abs, (void*)_open_file);

	int index_menor; //para guardar el menor item
	//creo una lista de key para guardar las key de cada file
	int len_key = 255;
	char** keys = malloc(sizeof(len_key)*cant_files);
	int _reduce_local(){
		int i=0;

		//cargo las keys, con su primer valor
		for(i=0;i<cant_files;i++){
			rs = getline(&(keys[i]), &len_key, fd[i]);
		}
		//aca ya tengo todas las keys

		while(alguna_key_distinta_null(keys, cant_files)){
			//obtengo cual es el menor
			index_menor = get_index_menor(keys, cant_files);
			//el menor lo mando a stdinn (keys[i])
			printf("%s\n", keys[index_menor]);
			write(PARENT_WRITE_FD, keys[index_menor], strlen(keys[index_menor])+1);
			//leo el siguiente elmento del fd[index_menor]
			len_key = 255;
			rs = getline(&(keys[index_menor]), &len_key, fd[index_menor]);

			//si es igual a -1, termino el file, marco como null la key para que la ignore cuando obtiene el menor
			if(rs==-1){
				keys[index_menor] = NULL;
			}
		}
		//si llego hasta aca termino de enviarle cosas por stdin,
		//cierro el stdin
		close(PARENT_WRITE_FD);

		int count;
		char* new_file_reduced;
		new_file_reduced = convertir_a_temp_path_filename(filename_result);//genero filename absoluto
		FILE* file_reduced = txt_open_for_append(new_file_reduced);//creo el file
		free_null((void*) &new_file_reduced);//limpio el nombre
		char* buffer = malloc(1024);//creo un buffer para ir almacenando el stdout
		i = 0;
		do {
			count = read(PARENT_READ_FD, buffer, 1024);
			fwrite(buffer, count, 1, file_reduced);
			i++;
		} while (count != 0);
		close(PARENT_READ_FD);
		fclose(file_reduced);
		free_null((void*) &buffer);


		return 0;
	}

	return (int)ejecutar_script(script_reduce, (void*) _reduce_local);
}

bool alguna_key_distinta_null(char** keys, int cant){
	bool rs = false;
	int i;
	for(i=0;i<cant;i++){
		if(keys[i] != NULL){
			rs = true;
			break;
		}
	}

	return rs;
}

int get_index_menor(char** keys, int cant){
	char key_men[255];
	memset(key_men, 255, 255);

	int index = 0;
	int i;
	for(i=0;i<cant;i++){
		if (keys[i] != NULL)
			if (strcmp(keys[i], key_men) < 0) {
				//key_men = keys[i];
				strcpy(key_men, keys[i]);
				index = i;
			}
	}
	return index;
}

int aplicar_reduce(t_list* files, char* script_reduce, char* filename_result){
	int i = 0;
	int _reduce(){
		int rs = 0;
		//cargo todos los archivos temporales para tenerlos a mano
		char** datafiles = malloc(list_size(files)*sizeof(char*));

		void _leer(t_files_reduce* fr) {
			//si son locales
			if (nodo_es_local(fr->ip, fr->puerto)) {//en el caso del combiner
				//obtengo el contenido al instante porque esta en el nodo
				datafiles[i] = getFileContent(fr->archivo);
			} else {
				//este seria en el caso del sin combiner o en el caso del reduce final
				//si no son locales tengo que buscar el file en otro nodo
				datafiles[i] = getFileContent_another_node(fr);
			}
			i++;
		}
		//
		list_iterate(files, (void*) _leer);

		//hasta aca tengo todos los archivos en memoria
		//creado para guardar todas las keys de cada archivo
		size_t len_keyvalue = 128;
		char* buff = malloc(len_keyvalue);
		char** key = malloc(list_size(files)*sizeof(char*));
		for (i = 0;i<list_size(files);i++) {
			key[i] = getline (&buff, &len_keyvalue, datafiles[i]);
		}




/*
		char* stdinn = read_whole_file(file_mapped_temp);
		//lo borro
		free_null((void*)&file_mapped_temp);

		size_t len = strlen(stdinn);
		write(PARENT_WRITE_FD, stdinn, len);

		//////////////////////////////////////////////////////////////////////////////////
		close(PARENT_WRITE_FD);
		char* buffer = malloc(len);
		int count;
		i =0;
		do {
			count = read(PARENT_READ_FD, buffer + i, 1);
			i++;
		} while (count != 0);
		close(PARENT_READ_FD);

		if (i >= 0) {
			buffer[i-1] = '\0';
			char* new_file_reduced;
			new_file_reduced = convertir_a_temp_path_filename(filename_result);

			grabar_en_temp(new_file_reduced, buffer);

			free_null((void*)&new_file_reduced);
			rs = 0;
		} else {
			printf("IO Error\n");
			rs = -1;
		}

		free_null((void*)&stdinn);
		free_null((void*)&buffer);
		*/

		return rs;
	}

	return (int)ejecutar_script(script_reduce, (void*) _reduce);
}


bool nodo_es_local(char* ip, int puerto){
	return string_equals_ignore_case(ip, NODO_IP()) && puerto == NODO_PORT();
}

char* convertir_a_temp_path_filename(char* filename){
	char* new_path_file = string_new();
	string_append(&new_path_file, NODO_DIRTEMP());
	string_append(&new_path_file, "/");
	string_append(&new_path_file, filename);
	return new_path_file;
}

/*
void* ejecutar_script(char* mapping, void* (*procesar_std)()) {
	// pipes for parent to write and read
	if(pipe(pipes[PARENT_READ_PIPE])==-1)
		handle_error("pipe");

	if((pipe(pipes[PARENT_WRITE_PIPE]))==-1)
		handle_error("pipe");
	char *argv[2] ;
	switch (fork()) {
	case -1:
		handle_error("fork");
		break;
	case 0: // CHILD
		argv[0] = mapping;
		argv[0] = NULL;

		dup2(CHILD_READ_FD, STDIN_FILENO);
		dup2(CHILD_WRITE_FD, STDOUT_FILENO);
		close(CHILD_READ_FD);
		close(CHILD_WRITE_FD);
		close(PARENT_READ_FD);
		close(PARENT_WRITE_FD);

        execv(argv[0], argv);
		break;
	default:
		break; // fall through to parent
	}

	close(CHILD_READ_FD);
	close(CHILD_WRITE_FD);

	return procesar_std();


}
*/

void* ejecutar_script(char* script, void* (*procesar_std)()) {
	// pipes for parent to write and read
	if (pipe(pipes[PARENT_READ_PIPE]) == -1)
		handle_error("pipe");

	if ((pipe(pipes[PARENT_WRITE_PIPE])) == -1)
		handle_error("pipe");

	int p =fork();
	if(p==-1)
		handle_error("fork");

	if (p==0) {
		char *argv[] = { script, NULL };
		dup2(CHILD_READ_FD, STDIN_FILENO);
		dup2(CHILD_WRITE_FD, STDOUT_FILENO);
		close(CHILD_READ_FD);
		close(CHILD_WRITE_FD);
		close(PARENT_READ_FD);
		close(PARENT_WRITE_FD);

		execv(argv[0], argv);
		return NULL;
	} else {
		close(CHILD_READ_FD);
		close(CHILD_WRITE_FD);

		return procesar_std();
	}
}

int aplicar_map(int n_bloque, char* script_map, char* filename_result){

	int _aplicar_map()
	{
		int res = 0;
		int count;

		// Write to child’s stdin
		char* stdinn = getBloque(n_bloque);
		size_t len = strlen(stdinn)+2;
		stdinn[len-1] = '\0';
		stdinn[len-2] = '\n';
		write(PARENT_WRITE_FD, stdinn, len);
		close(PARENT_WRITE_FD);
		free_null((void**)&stdinn);

		// Read from child’s stdout
		char* new_file_map_disorder = convertir_a_temp_path_filename(filename_result);
		string_append(&new_file_map_disorder , "-disorder.tmp");
		FILE* file_disorder = txt_open_for_append(new_file_map_disorder);

		char* buffer = malloc(1024);
		do{
			count = read(PARENT_READ_FD, buffer , 1024);
			fwrite(buffer, count, 1, file_disorder);
		}while(count!=0);
		close(PARENT_READ_FD);
		txt_close_file(file_disorder);
		free_null((void**)&buffer);
		ordenar_y_guardar_en_temp(new_file_map_disorder, filename_result);//guardo el file definitivo en el tmp
		free_null((void*)&new_file_map_disorder);

		return res;
	}

	return (int) ejecutar_script(script_map, (void*)_aplicar_map);
}

int ordenar_y_guardar_en_temp(char* file_desordenado, char* destino){

	char* commando_ordenar = string_new();
	string_append(&commando_ordenar, "cat ");
	string_append(&commando_ordenar, file_desordenado);
	string_append(&commando_ordenar, " | sort > ");
	string_append(&commando_ordenar, NODO_DIRTEMP());
	string_append(&commando_ordenar, "/");
	string_append(&commando_ordenar, destino);

	printf("%s\n", commando_ordenar);

	system(commando_ordenar);
	free_null((void*)&commando_ordenar);

	//borro el file
	remove(file_desordenado);
	return 0;
}

/*
char* ejecutar_script_sort(char* filename){
	int fi[2];
	int fo[2];
	int oldstdin;
	int oldstdout;

	if (pipe(fi) == -1) {
		exit(EXIT_FAILURE);
	}
	if (pipe(fo) == -1) {
		exit(EXIT_FAILURE);
	}
	oldstdin = dup(STDIN_FILENO);
	oldstdout = dup(STDOUT_FILENO);

	close(STDIN_FILENO);
	close(STDOUT_FILENO);

	if (dup2(fo[0], STDIN_FILENO) == -1)
		exit(EXIT_FAILURE);
	if (dup2(fi[1], STDOUT_FILENO) == -1)
		exit(EXIT_FAILURE);

	switch (fork()) {
	case -1:
		exit(EXIT_FAILURE);
	case 0:
		close(fo[0]);
		close(fo[1]);
		close(fi[0]);
		close(fi[1]);
		execlp("sort", "sort", (char *) NULL);
		break;
	default:
		break;
	}


	close(STDIN_FILENO);
	close(STDOUT_FILENO);
	dup2(oldstdin, STDIN_FILENO);
	dup2(oldstdout, STDOUT_FILENO);

	close(fo[0]);
	close(fi[1]);

	char* stdinn = read_whole_file(filename);
	size_t len = strlen(stdinn);
	stdinn[len] = '\0';
	//write(fo[1], "dino\nbat\nfish\nzilla\nlizard\n\0", 27);
	write(fo[1], stdinn, len);
	close(fo[1]);
	char* buffer = malloc(27);
	//buffer[read(fi[0], buffer, 27)] = 0;
	buffer[read(fi[0], buffer, len)] = 0;
	printf("%s\n", buffer);

	char* new_file_mapped_tidy = grabar_en_temp("job_map_tidy_", buffer);

	return new_file_mapped_tidy;
}
*/
int grabar_en_temp(char* filename, char* data){

	write_file(filename, data, strlen(data));
	printf("archivo temporal creado: %s\n", filename);

	return 0;

/*
	char* nombre_nuevo_archivo = NULL;
	nombre_nuevo_archivo = file_combine(NODO_DIRTEMP(), filename);
	char* timenow = temporal_get_string_time();
	string_append(&nombre_nuevo_archivo, timenow);
	free_null((void*) &timenow);

	write_file(nombre_nuevo_archivo, data, strlen(data));

	printf("archivo creado: %s\n", nombre_nuevo_archivo);
	//free_null(&nombre_nuevo_archivo);
	return nombre_nuevo_archivo;
	*/
}

void agregar_cwd(char* file){
	char cwd[1024];
	if (getcwd(cwd, sizeof(cwd)) == NULL)
		handle_error("getcwd() error");

	char* aux = malloc(strlen(file));
	strcpy(aux, file);
	strcpy(file, cwd);
	strcat(file, aux);
	free(aux);
}


void iniciar_server_thread() {
	pthread_t th;
	pthread_attr_t tattr;

	pthread_attr_init(&tattr);
	pthread_attr_setdetachstate(&tattr, PTHREAD_CREATE_DETACHED);
	pthread_create(&th, &tattr, (void*) incicar_server, NULL);
	pthread_attr_destroy(&tattr);

}

void procesar_mensaje(int fd, t_msg* msg) {
	int rs;
	print_msg(msg);
	int n_bloque = 0;
	char* buff;

	switch (msg->header.id) {
	case NODO_GET_FILECONTENT:
		//lo convierto a path absoluto
		buff = convertir_a_temp_path_filename(msg->stream);
		//obtengo el char
		char* file = getFileContent(buff);
		free_null((void*)&buff);

		//envio el archivo
		destroy_message(msg);
		msg = string_message(NODO_GET_FILECONTENT, file, 0);
		if((rs = enviar_mensaje(fd, msg))<0){
			printf("Error al arhivo temp NODO_GET_FILECONTENT\n");
			perror("enviar_mensaje ");
		}
		destroy_message(msg);

		break;
	case NODO_GET_BLOQUE:
		n_bloque = msg->argv[0];
		destroy_message(msg);
		msg = string_message(NODO_GET_BLOQUE, getBloque(n_bloque), 0);//en la posicion 0 esta en nuemro de bloque
		enviar_mensaje(fd, msg);

		destroy_message(msg);

		break;
	case NODO_HOLA:
		destroy_message(msg);
		msg = string_message(NODO_HOLA, "", 0);
		enviar_mensaje(fd, msg);
		destroy_message(msg);
		break;
	case NODO_CHAU:
		FIN = true;
		destroy_message(msg);

		break;
	case FS_HOLA:

		destroy_message(msg);
		msg = string_message(NODO_HOLA, "", 0);
		enviar_mensaje(fd, msg);
		destroy_message(msg);

		//recibo el mensaje con el bloque a grabar, y la posiocion en el arg 0
		msg = recibir_mensaje(fd);
		print_msg(msg);

		char* bloque = malloc(TAMANIO_BLOQUE_B);
		memcpy(bloque, msg->stream, msg->argv[1]);	//1 es el tamaño real
		memset(bloque + msg->argv[1], '\0', TAMANIO_BLOQUE_B - msg->argv[1]);
		setBloque(msg->argv[0], bloque);
		free_null((void*)&bloque);

		destroy_message(msg);

		break;
	default:
		break;
	}

}
void incicar_server() {
	printf("Iniciado server nodo para FS y JOB. Puerto: %d\n", NODO_PORT());

	server_socket_select(NODO_PORT(), procesar_mensaje);
}

int NODO_CANT_BLOQUES() {
	return CANT_BLOQUES;
}

void probar_conexion_fs() {

	log_trace(logger, "conectado al FS ... ");

	int fs;

	if ((fs = client_socket(NODO_IP_FS(), NODO_PORT_FS())) > 0) {
		printf("Conectado\n");
		//t_msg* msg= id_message(NODO_CONECTAR_CON_FS);
		t_msg* msg = NULL;
		msg = string_message(NODO_CONECTAR_CON_FS, "hola soy el NodoA", 0);
		printf("enviavndo mensaje al fs\n");
		if (enviar_mensaje(fs, msg) < 0) {
			puts("ERROR: Se ha perdido la conexión con el fs.");
			exit(EXIT_FAILURE);
		}
		destroy_message(msg);

		printf("recibiendo respueta\n");
		if ((msg = recibir_mensaje(fs)) == NULL) {
			puts("ERROR: Se ha perdido la conexión con el fs.\n");
			exit(EXIT_FAILURE);
		}

		print_msg(msg);
		if (msg->header.id == FS_NODO_QUIEN_SOS) {
			printf("el fs me pregunta quien soy, le digo mi ip y port\n");

			destroy_message(msg);

			/*
			 t_nodo* nodo = nodo_new(NODO_IP(), NODO_PORT(), NODO_NUEVO(), NODO_CANT_BLOQUES());
			 enviar_mensaje_flujo(fs, 1, sizeof(t_nodo), nodo);*/

			msg = string_message(RTA_FS_NODO_QUIEN_SOS, NODO_IP(), 3,
					NODO_PORT(), NODO_NUEVO(), NODO_CANT_BLOQUES());
			if ((enviar_mensaje(fs, msg)) < 0) {
				printf("No se pudo responder a %s",
						id_string(FS_NODO_QUIEN_SOS));
				exit(EXIT_FAILURE);
			}
			destroy_message(msg);

			printf("mensaje de ip y puerto enviado\n");

		} else
			printf("No se pudo conectar con el fs");

		close(fs);
		printf("Conectado con fs en %s:%d\n", NODO_IP_FS(), NODO_PORT_FS());
	} else {
		printf("No pudo iniciar la escucha al fs\n");
	}

}
void finalizar() {
	data_destroy();
	config_destroy(config);
	log_destroy(logger);

	printf("fin ok\n");
	//while (true);
}

void inicializar() {
	/*
	 * deberia tomar el getcwd y concatenarle el nombre de archivo
	 char cwd[1024];
	 getcwd(cwd, sizeof(cwd));
	 char* file_config = file_combine(cwd, FILE_CONFIG);
	 */
	agregar_cwd(FILE_CONFIG);
	config = config_create(FILE_CONFIG);
	agregar_cwd(FILE_LOG);
	logger = log_create(FILE_LOG, "Nodo", true, LOG_LEVEL_INFO);

	_data = data_get(NODO_ARCHIVOBIN());
}

/*
 * devuelve un puntero con el archivo mapeado
 */
char* getFileContent(char* filename) {
	log_info(logger, "Inicio getFileContent(%s)", filename);
	char* content = NULL;

	//creo el espacio para almacenar el archivo
	char* path = file_combine(NODO_DIRTEMP(), filename);
	size_t size = file_get_size(path)+1;
	content = malloc(size);

	char* mapped=NULL;
	mapped = file_get_mapped(path);
	memcpy(content, mapped, size);//
	file_mmap_free(mapped, path);

	free_null((void*)&path);

	log_info(logger, "Fin getFileContent(%s)", filename);
	return content;

	/*log_info(logger, "Inicio getFileContent(%s)", filename);
	char* content = NULL;
	//creo el espacio para almacenar el archivo
	char* path = file_combine(NODO_DIRTEMP(), filename);
	content = malloc(file_get_size(path));

	content = file_get_mapped(path);

	free_null((void*)&path);

	log_info(logger, "Fin getFileContent(%s)", filename);
	return content;*/
}

void setBloque(int32_t numero, char* bloquedatos) {
	log_info(logger, "Inicio setBloque(%d)", numero);

	//memcpy((void*)(_data[numero*TAMANIO_BLOQUE]), bloquedatos, TAMANIO_BLOQUE);
	memcpy(_data + (numero * TAMANIO_BLOQUE_B), bloquedatos, TAMANIO_BLOQUE_B);

	log_info(logger, "Fin setBloque(%d)", numero);
}
/*
 * devuelve una copia del bloque, hacer free
 */
char* getBloque(int32_t numero) {
	log_info(logger, "Ini getBloque(%d)", numero);
	void* bloque = NULL;
	bloque = malloc(TAMANIO_BLOQUE_B);
	memcpy(bloque, &(_data[numero * TAMANIO_BLOQUE_B]), TAMANIO_BLOQUE_B);
	//memcpy(bloque, _bloques[numero], TAMANIO_BLOQUE);
	log_info(logger, "Fin getBloque(%d)\n", numero);
	return bloque;
}

//devuelvo el archivo data.bin mappeado
void* data_get(char* filename) {
	char filenamenew[1024];
	strcpy(filenamenew, filename);
	agregar_cwd(filenamenew);

	if (!file_exists(filenamenew)) {
		TAMANIO_DATA = 1024 * 1024 * 500; //100MB
		FILE* file = NULL;
		file = fopen(filenamenew, "w+");
		if (file == NULL) {
			handle_error("fopen");
		}

		printf("creando archivo de %d bytes ...\n", TAMANIO_DATA);
		//lo creo con el tamaño maximo
		void* dump = NULL;
		dump = malloc(TAMANIO_DATA);

		////grabo 0 en todo el nodo.
		memset(dump, 0, TAMANIO_DATA);
		fwrite(dump, TAMANIO_DATA, 1, file);
		free_null((void*)&dump);

		fclose(file);
	}
	//calculo la cantidad de bloques
	TAMANIO_DATA = file_get_size(filenamenew);
	CANT_BLOQUES = TAMANIO_DATA / TAMANIO_BLOQUE_B;

//el archivo ya esta creado con el size maximo
	return file_get_mapped(filenamenew);
}

void data_destroy() {
	munmap(_data, TAMANIO_DATA);
//mapped = NULL;
}


