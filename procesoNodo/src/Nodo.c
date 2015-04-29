#include <stdio.h>
#include <stdlib.h>
#include <commons/config.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <util.h>
#include <commons/string.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <commons/log.h>
#include <pthread.h>

#include "socket.h"

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
t_config* _config = NULL;
t_log* _log = NULL;
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

void fs_conectar();
/*
 * main
 */

int main(void) {
	int i;

	/*
	 * deberia tomar el getcwd y concatenarle el nombre de archivo
	 char cwd[1024];
	 getcwd(cwd, sizeof(cwd));
	 char* file_config = file_combine(cwd, FILE_CONFIG);
	 _config = config_create(file_config);
	 free(file_config);
	 _log = log_create(FILE_LOG, "Nodo", false, LOG_LEVEL_INFO);*/
	_config = config_create(FILE_CONFIG);
	_log = log_create(FILE_LOG, "Nodo", false, LOG_LEVEL_INFO);

	//char* file_data = file_combine(cwd, config_get_string_value(_config, CONFIG_ARCHIVO_BIN));
	//_data = data_get(file_data);
	_data = data_get(config_get_string_value(_config, CONFIG_ARCHIVO_BIN));

	//free(file_data);
	bloques_set();
	////////////////////////

	pthread_t p_fs;
	if (pthread_create(&p_fs, NULL, (void*) fs_conectar, NULL) != 0) {
		perror("pthread_create");
		exit(1);
	}

	pthread_join(p_fs, (void**) NULL);

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
	data_destroy();
	config_destroy(_config);

	printf("fin ok\n");
	//while (true);
	return EXIT_SUCCESS;
}

void fs_conectar() {
	printf("conectado al FS \n");

	//conecto al fs
	int fs;
	fs = quieroUnPutoSocketAndando(
			config_get_string_value(_config, CONFIG_IP_FS),
			config_get_int_value(_config, CONFIG_PUERTO_FS));

	char* auxC;
	int len = strlen("hola") + 1;
	auxC = malloc(len);
	strcpy(auxC, "hola");
	//handshake Orquestador-Personaje
	if (mandarMensaje(fs, 1, len, auxC) > 0) {
		if (recibirMensaje(fs, (void**) &auxC) > 0) {
			printf("Handshake contestado %s\n", auxC);
			//log_debug("Handshake contestado del Orquestador %c", *auxC);
		}
	}
	close(fs);

}

/*
 * devuelve un puntero con el archivo mapeado
 */
void* getFileContent(char* filename) {
	log_info(_log, "Inicio getFileContent(%s)", filename);
	void* content = NULL;

//creo el espacio para almacenar el archivo
	char* path = file_combine(config_get_string_value(_config, CONFIG_DIR_TEMP),
			filename);
	content = malloc(file_get_size(path));

	content = file_get_mapped(path);

	free(path);

	log_info(_log, "Fin getFileContent(%s)", filename);
	return content;
}

void bloques_set() {
	int i = 0;
	for (i = 0; i < 50; i++) {
		_bloques[i] = (_data + (i * BLOQUE_SIZE));
	}
}

void setBloque(int32_t numero, void* bloquedatos) {
	log_info(_log, "Inicio setBloque(%d)", numero);
//printf("___\n");
//printf("%p\n", bloques[numero]);
	memcpy(_bloques[numero], bloquedatos, BLOQUE_SIZE);

	log_info(_log, "Fin setBloque(%d)", numero);
}
/*
 * devuelve una copia del bloque, hacer free
 */
void* getBloque(int32_t numero) {
	log_info(_log, "Ini getBloque(%d)", numero);
	void* bloque = NULL;
	bloque = malloc(BLOQUE_SIZE);
//memcpy(bloque,&(data[numero*BLOQUE_SIZE]), BLOQUE_SIZE);
	memcpy(bloque, _bloques[numero], BLOQUE_SIZE);
	log_info(_log, "Fin getBloque(%d)", numero);
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

