#ifndef REDUCE_H_
#define REDUCE_H_

	#include <stdio.h>
	#include <stdlib.h>
	#include <stdbool.h>
	#include <string.h>

	#define ABC_LENGTH ('z'-'a'+1)
	#define KEY_VALUE_SEPARATOR "=>"

	typedef struct{
		char character;
		int repetitions;
	} t_key_value;

	typedef struct{
		size_t size;
		t_key_value dictionary[ABC_LENGTH];
	} t_static_dictionary;

	// Iterar entre la entrada, e ir reduciendo. Es resbonsalibidad de la funcíon de reduccion, imprimir. La ultima impresión está forzada
	int reduce(FILE*, FILE*, int (*)(FILE*, const char const*, void**, int (*)(FILE*, void*)), int (*)(FILE*, void*));

	/**
	 * Formato: Un numero por fila
	 * Suma cada fila. No imprime sumas parciales
	 */
	int reduce_simple_sum(const char const*, void**, int (*)(FILE*, void*));

	/**
	 * Imprime la suma total, como un único numero
	 */
	int reduce_simple_sum_spit(FILE*, void*);

	/**
	 * Formato: Letra miniscula KEY_VALUE_SEPARATOR (=>) cantidad de repeticiones
	 * Suma las repeticiones, siempre que la letra sea la misma. Si cambia, imprime la suma anterior.
	 */
	int reduce_static_dictionary_sum(FILE*, const char const*, t_static_dictionary**, int (*)(FILE*, t_static_dictionary*));

	/**
	 * Imprime en el formato anterior
	 */
	int reduce_static_dictionary_sum_spit(FILE*, t_static_dictionary*);

#endif
