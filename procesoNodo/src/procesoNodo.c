#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <commons/string.h>
#include <commons/temporal.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <commons/log.h>
#include <pthread.h>
#include "config_nodo.h"
#include <nodo.h>
#include <strings.h>

#include <util.h>
//#include "socket.h"

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

void* getFileContent(char* filename);

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
int aplicar_reduce(char* file, char* script_reduce, char* filename_result);
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
/*
	char* timenow = temporal_get_string_time();

	int n_bloque =0;
	char* file_map = string_new();
	string_append(&file_map, "job_map_");
	string_append(&file_map, timenow);
	string_append(&file_map, ".txt");

	aplicar_map(n_bloque, "/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoJob/map.sh", file_map);
	printf("%s\n", file_map);


	char* file_reduce = string_new();
	string_append(&file_reduce, "job_reduce_");
	string_append(&file_reduce, timenow);
	aplicar_reduce(file_map, "/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoJob/reduce.sh", file_reduce);
	printf("%s\n", file_reduce);
	free(file_map);
	free(file_reduce);

	free_null((void*) &timenow);*/

	//bool fin = true	;
	while (!FIN);
	finalizar();

	return EXIT_SUCCESS;
}

int aplicar_reduce(char* file_mapped, char* script_reduce, char* filename_result){

	int _reduce(){
		int rs = 0;
		//lo convierto a path absoluto para leer el file
		char* file_mapped_temp = convertir_a_temp_path_filename(file_mapped);

		char* stdinn = read_whole_file(file_mapped_temp);
		//lo borro
		free_null((void*)&file_mapped_temp);

		size_t len = strlen(stdinn);
		write(PARENT_WRITE_FD, stdinn, len);
		close(PARENT_WRITE_FD);

		char* buffer = malloc(len);
		int count;
		int i = 0;
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

		return rs;
	}

	return (int)ejecutar_script(script_reduce, (void*) _reduce);
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
		char* buffer = malloc(TAMANIO_BLOQUE_B);
		int res = 0;
		int count;

		// Write to child’s stdin
		char* stdinn = getBloque(n_bloque);
		size_t len = strlen(stdinn)+2;
		stdinn[len-1] = '\0';
		stdinn[len-2] = '\n';
		write(PARENT_WRITE_FD, stdinn, len);
		close(PARENT_WRITE_FD);

		// Read from child’s stdout
		int i=0;
		do{
			count = read(PARENT_READ_FD, buffer + i, 1);
			i++;
		}while(count!=0);
		close(PARENT_READ_FD);

		if (i >= 0) {
			buffer[i-1] = '\0';//-1 porque el ultimo es basura
			//grabo el archivo, primero lo guardo en un .tmp y luego lo ordeno
			char* new_file_map_disorder = convertir_a_temp_path_filename(filename_result);
			string_append(&new_file_map_disorder , "-disorder.tmp");
			//guardo en temp el archivo mapeado y desordenado
			//grabar_en_temp(new_file_map_disorder ,buffer);
			write_file(new_file_map_disorder, buffer, strlen(buffer));

			//ordeno el archivo y lo guardo con el nombre final
			ordenar_y_guardar_en_temp(new_file_map_disorder, filename_result);

			free_null((void*)&new_file_map_disorder);

			res = 0;
			//system("cat /tmp/map.py.result.tmp | sort > /tmp/librazo12347.tmp");
			//res = 0;
		} else {
			printf("IO Error\n");
			res = -1;
		}

		free_null((void**)&stdinn);
		free_null((void**)&buffer);
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
	print_msg(msg);
	int n_bloque = 0;

	switch (msg->header.id) {
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
void* getFileContent(char* filename) {
	log_info(logger, "Inicio getFileContent(%s)", filename);
	void* content = NULL;

//creo el espacio para almacenar el archivo
	char* path = file_combine(NODO_DIRTEMP(), filename);
	content = malloc(file_get_size(path));

	content = file_get_mapped(path);

	free_null((void*)&path);

	log_info(logger, "Fin getFileContent(%s)", filename);
	return content;
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


