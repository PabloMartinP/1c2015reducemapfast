#ifndef MAP_H_
#define MAP_H_

	#include <stdio.h>
	#include <stdlib.h>
	#include <stdbool.h>
	#include <string.h>
	#include <ctype.h>

	#define ABC_LENGTH ('z'-'a'+1)
	#define KEY_VALUE_SEPARATOR "=>"

	//Itera cada linea y aplica el mapeo
	int map(FILE*, FILE*, int (*)(const char const*, FILE*));

	/**
	 * Formato entrada: Texto plano
	 * Formato salida: cantidad de encuentros de la palabra
	 * Por cada linea, imprime el numero de ocurrencias.
	 *
	 * Notar que la funcion tiene 3 parametros, y el iterador solo admite funciones con 2, por lo que se debe generar una funcion auxiliar como:
	 * * int map_contar_MaRTA(const char const* input, FILE* stream_out){
	 * *	return map_contar_ocurrencias(input, stream_out, "MaRTA");
	 * * }
	 */
	int map_count_occurrences(const char const*, FILE*, const char const *);
	/**
	 * Formato entrada: Texto plano
	 * Formato salida: Letra KEY_VALUE_SEPARATOR (=>) repeticiones
	 * Por cada linea, imprime todas las letras y sus repeticiones (inclusive si no hay ningúna)
	 */
	int map_count_leters(const char const*, FILE*);

#endif
