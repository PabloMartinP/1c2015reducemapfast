/*
 ============================================================================
 Name        : procesoFileSystem.c
 Author      : 
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include <commons/string.h>
#include <strings.h>

const int COMAND_MAX_SIZE = 256;
const char* FILE_DIRECTORIO = "directorio.txt";
const char* FILE_ARCHIVO = "archivo.txt";

typedef enum {
	NODO_AGREGAR, NODO_ELIMINAR,
	FORMATEAR,
	NADA, SALIR,
	DIRECTORIO_CREAR, DIRECTORIO_RENOMBRAR, DIRECTORIO_ELIMINAR, DIRECTORIO_MOVER, DIRECTORIO_LISTAR,
	ARCHIVO_COPIAR_LOCAL_MDFS, COPIAR_RENOMBRAR, ARCHIVO_ELIMINAR, ARCHIVO_MOVER, ARCHIVO_LISTAR, ARCHIVO_COPIAR_MDFS_LOCAL
} e_comando;

typedef struct {
	int index;
	char nombre[128];
	int padre;
} t_directorio;
typedef struct {
	char nombre[128];
	long int tamanio;
	int directorio;
	bool estado;
	//falta la lista de nodos
} t_archivo;

e_comando getComando(char* input_user) {
	char* comando;

	//obtener el nombre del comando que ingreso el user
	comando = string_split(input_user, " ")[0];

	if (string_equals_ignore_case(comando, "agregarnodo\n"))	return NODO_AGREGAR;
	if (string_equals_ignore_case(comando, "eliminarnodo\n"))		return NODO_ELIMINAR;
	if (string_equals_ignore_case(comando, "mkdir\n"))			return DIRECTORIO_CREAR;
	if (string_equals_ignore_case(comando, "formatear\n"))			return FORMATEAR;
	if (string_equals_ignore_case(comando, "salir\n"))			return SALIR;

	return NADA;
}

void formatear();
void directorio_crear(char* comando);

int main(void) {
	puts("!!!Hello World!!!"); /* prints !!!Hello World!!! */
	char comando[COMAND_MAX_SIZE];

	printf("inicio consola\nIngresar comandos");

	bool fin = false;
	while (!fin) {
		fgets(comando, COMAND_MAX_SIZE, stdin);

		switch (getComando(comando)) {
		case NODO_AGREGAR:
			printf("comando ingresado: agregar nodo\n");

			//
			break;
		case NODO_ELIMINAR:
			printf("comando ingresado: elimnar nodo\n");

			break;
		case DIRECTORIO_CREAR:
			printf("crear directorio");

			directorio_crear(comando);
			break;
		case FORMATEAR:
			formatear();
			break;
		case SALIR:
			printf("comando ingresado: salir\n");
			fin = true;
			break;
		default:
			printf("comando desconocido\n");
			break;
		}

	}

	printf("bb world!!!!");
	return EXIT_SUCCESS;
}

void directorio_crear(char* comando){
	FILE* file = fopen(FILE_DIRECTORIO, "a+");

	t_directorio dir;
	dir.index = 1;//dir_ultimoIndex();
	strcpy(dir.nombre, "un directorio")	;
	dir.padre = 0;

	//fwrite(&dir, sizeof(t_directorio), 1, file);
	fprintf(file, "hayque grabar el directorio");



	fclose(file);

	printf("grabo el dir\n");
}


void formatear(){
	FILE* file1 = fopen(FILE_DIRECTORIO, "w+");
	FILE* file2 = fopen(FILE_ARCHIVO, "w+");
	fclose(file1);
	fclose(file2);
	printf("se formateo\n");
}

