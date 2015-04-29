/*
 * consola.c
 *
 *  Created on: 29/4/2015
 *      Author: utnso
 */

#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <commons/string.h>

#include "consola.h"

void iniciar_consola() {
	char comando[COMMAND_MAX_SIZE];
	/*
	 * INICIO CONSOLA
	 */
	printf("inicio consola\nIngresar comandos  \n");

	bool fin = false;
	while (!fin) {
		fgets(comando, COMMAND_MAX_SIZE, stdin);

		switch (getComando(comando)) {
		case NODO_AGREGAR:
			printf("comando ingresado: agregar nodo\n");

			//
			break;
		case NODO_ELIMINAR:
			printf("comando ingresado: elimnar nodo\n");

			break;
		case DIRECTORIO_CREAR:
			printf("crear directorio\n");

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
}


e_comando getComando(char* input_user) {
	char* comando;

//obtener el nombre del comando que ingreso el user
	comando = string_split(input_user, " ")[0];

	if (string_equals_ignore_case(comando, "agregarnodo\n"))
		return NODO_AGREGAR;
	if (string_equals_ignore_case(comando, "eliminarnodo\n"))
		return NODO_ELIMINAR;
	if (string_equals_ignore_case(comando, "mkdir\n"))
		return DIRECTORIO_CREAR;
	if (string_equals_ignore_case(comando, "formatear\n"))
		return FORMATEAR;
	if (string_equals_ignore_case(comando, "salir\n"))
		return SALIR;

	return NADA;
}

void directorio_crear(char* comando) {
	FILE* file = fopen(FILE_DIRECTORIO, "a+");

	t_directorio dir;
	dir.index = 1; //dir_ultimoIndex();
	strcpy(dir.nombre, "un directorio");
	dir.padre = 0;

//fwrite(&dir, sizeof(t_directorio), 1, file);
	fprintf(file, "hayque grabar el directorio");

	fclose(file);

	printf("grabo el dir\n");
}

void formatear() {
	FILE* file1 = fopen(FILE_DIRECTORIO, "w+");
	FILE* file2 = fopen(FILE_ARCHIVO, "w+");
	fclose(file1);
	fclose(file2);
	printf("se formateo\n");
}

