#include <stdio.h>
#include <stdlib.h>
#include <commons/config.h>
#include <sys/stat.h>
#include <sys/mman.h>

#include "util.h"

#include <commons/string.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <commons/log.h>
#include <pthread.h>

//#include "socket.h"

#define CONFIG_ARCHIVO_BIN  "ARCHIVO_BIN"
#define CONFIG_DIR_TEMP  "DIR_TEMP"
#define CONFIG_PUERTO_FS  "PUERTO_FS"
#define CONFIG_IP_FS  "IP_FS"

#define FILE_CONFIG "/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoNodo/config.txt"
#define FILE_LOG "/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoNodo/log.txt"
/*
 * variables
 */

int DATA_SIZE = 1024 * 1024 * 50; //50MB
int BLOQUE_SIZE = 1024 * 1024 * 20; //20mb

void* _bloques[50];
void* _data = NULL;
t_config* config = NULL;
t_log* logger = NULL;
/*
 * declaraciones
 */
void* data_get(char* filename);
void data_destroy();
void* getBloque(int32_t numero);
void setBloque(int32_t numero, void* bloque);
//void setBloque(int32_t numero, char* bloque);
void bloques_set();
void* getFileContent(char* filename);

void inicializar();
void finalizar();

void conectar_con_fs();
/*
 * main
 */

int main(void) {

	inicializar();

	conectar_con_fs();

	/*
	 //settear el bloque 0
	 void* saludo = malloc(BLOQUE_SIZE);
	 strcpy(saludo, "ahora cambio el mensaje!");
	 setBloque(0, saludo);

	 //leo el bl0que 0
	 void* dataget = getBloque(0);
	 char* saludoget =(char*) malloc(strlen(saludo)+1);
	 memcpy(saludoget, dataget, strlen(saludo)+1);
	 printf("%s\n", saludoget);

	 free_null(saludo);
	 free_null(saludoget);
	 free_null(dataget);
	 */

	/*
	 char *d = NULL;
	 d = getFileContent("hola");

	 for(i=0;i<10;i++)
	 printf("%c", d[i]);

	 file_mmap_free(d, "hola");
	 */
////
	finalizar();

	return EXIT_SUCCESS;
}

void conectar_con_fs() {
	/*
	 pthread_t p_fs;
	 if (pthread_create(&p_fs, NULL, (void*) fs_conectar, NULL) != 0) {
	 perror("pthread_create");
	 exit(1);
	 }
	 pthread_join(p_fs, (void**) NULL);*/

	log_trace(logger, "conectado al FS ... ");

	int fs;
	int port = config_get_int_value(config, CONFIG_PUERTO_FS);
	char* ip = config_get_string_value(config, CONFIG_IP_FS);

	if ((fs = client_socket(ip, port)) > 0) {
		printf("Conectado\n");
		//t_msg* msg= id_message(NODO_CONECTAR_CON_FS);
		t_msg* msg = NULL;
		char* saludo = strdup("hola soy el NodoA");
		msg = string_message(NODO_CONECTAR_CON_FS, saludo, 0);
		printf("enviar mensaje\n");
		if (enviar_mensaje(fs, msg) < 0) {
			puts("ERROR: Se ha perdido la conexión con el fs.");
			exit(EXIT_FAILURE);
		}

		destroy_message(msg);

		printf("recibir mensaje\n");
		if ((msg = recibir_mensaje(fs)) == NULL) {
			puts("ERROR: Se ha perdido la conexión con el fs.");
			exit(EXIT_FAILURE);
		}

		print_msg(msg);
		if (msg->header.id == FS_NODO_OK) {
			printf("Conectado con FS OK!!!");

			destroy_message(msg);
			//msg = id_message(NODO_SALIR);
			msg = string_message(NODO_SALIR, "OK", 0);
			enviar_mensaje(fs, msg);
			printf("mensaje de desconexion enviado\n");
			destroy_message(msg);
		} else
			printf("No se pudo conectar con el fs");

		close(fs);
	}
	else	{
		printf("No pudo iniciar la escucha al fs\n");
	}

}
void finalizar() {
	data_destroy();
	config_destroy(config);

	printf("fin ok\n");
	//while (true);
}
void inicializar() {
	/*
	 * deberia tomar el getcwd y concatenarle el nombre de archivo
	 char cwd[1024];
	 getcwd(cwd, sizeof(cwd));
	 char* file_config = file_combine(cwd, FILE_CONFIG);
	 _config = config_create(file_config);
	 free(file_config);
	 _log = log_create(FILE_LOG, "Nodo", false, LOG_LEVEL_INFO);*/
	config = config_create(FILE_CONFIG);
	logger = log_create(FILE_LOG, "Nodo", true, LOG_LEVEL_INFO);

	//char* file_data = file_combine(cwd, config_get_string_value(_config, CONFIG_ARCHIVO_BIN));
	//_data = data_get(file_data);
	_data = data_get(config_get_string_value(config, CONFIG_ARCHIVO_BIN));

	//free(file_data);
	bloques_set();

}

/*
 * devuelve un puntero con el archivo mapeado
 */
void* getFileContent(char* filename) {
	log_info(logger, "Inicio getFileContent(%s)", filename);
	void* content = NULL;

//creo el espacio para almacenar el archivo
	char* path = file_combine(config_get_string_value(config, CONFIG_DIR_TEMP),
			filename);
	content = malloc(file_get_size(path));

	content = file_get_mapped(path);

	free(path);

	log_info(logger, "Fin getFileContent(%s)", filename);
	return content;
}

void bloques_set() {
	int i = 0;
	for (i = 0; i < 50; i++) {
		_bloques[i] = (_data + (i * BLOQUE_SIZE));
	}
}

void setBloque(int32_t numero, void* bloquedatos) {
	log_info(logger, "Inicio setBloque(%d)", numero);
//printf("___\n");
//printf("%p\n", bloques[numero]);
	memcpy(_bloques[numero], bloquedatos, BLOQUE_SIZE);

	log_info(logger, "Fin setBloque(%d)", numero);
}
/*
 * devuelve una copia del bloque, hacer free
 */
void* getBloque(int32_t numero) {
	log_info(logger, "Ini getBloque(%d)", numero);
	void* bloque = NULL;
	bloque = malloc(BLOQUE_SIZE);
//memcpy(bloque,&(data[numero*BLOQUE_SIZE]), BLOQUE_SIZE);
	memcpy(bloque, _bloques[numero], BLOQUE_SIZE);
	log_info(logger, "Fin getBloque(%d)", numero);
	return bloque;
}

//devuelvo el archivo data.bin mappeado
void* data_get(char* filename) {

	if (!file_exists(filename)) {
		FILE* file = NULL;
		file = fopen(filename, "w+");
		if (file == NULL) {
			handle_error("fopen");
		}

		printf("creado\n");
		//lo creo con el tamaño maximo
		void* dump = NULL;
		dump = malloc(DATA_SIZE);

		memset(dump, 0, DATA_SIZE);
		fwrite(dump, DATA_SIZE, 1, file);
		free_null(dump);

		fclose(file);
	}

//el archivo ya esta creado con el size maximo
	return file_get_mapped(filename);
}

void data_destroy() {
	munmap(_data, DATA_SIZE);
//mapped = NULL;
}

