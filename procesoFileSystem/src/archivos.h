/*
 * archivos.h
 *
 *  Created on: 2/5/2015
 *      Author: utnso
 */

#ifndef ARCHIVOS_H_
#define ARCHIVOS_H_

#include <commons/txt.h>
#include <stdbool.h>
#include <commons/collections/list.h>
#include <nodo.h>
#include <util.h>
#include <libgen.h>

/*
char FILE_ARCHIVO[1024] ="/mdfs_archivos.bin";
char FILE_ARCHIVO_BLOQUES[1024] ="/mdfs_bloques.bin";
*/
#define FILE_ARCHIVO "mdfs_archivos.bin"
#define FILE_ARCHIVO_BLOQUES "mdfs_bloques.bin"

typedef struct { //estructura que tiene las tres copias del bloque
	int n_bloque; //numero de bloque
	t_list* nodosbloque; //tiene tres estructuras t_nodo_bloque
} t_bloque_de_datos;

typedef struct {
	int id;
	char nombre[128];
	size_t tamanio;
	int directorio;
	bool estado;
	int cant_bloques;
} t_archivo_info;

typedef struct {
	t_archivo_info* info;
	t_list* bloques_de_datos; //aca guardo el nodo y el bloque a donde estan guardados los datos
} t_archivo;

void arch_agregar(t_archivo* archivo);
t_bloque_de_datos* bloque_de_datos_create();
t_archivo_info* arch_get_info(char* nombre, int dir_padre) ;
void arch_formatear();
t_archivo* arch_crear();
void arch_print(t_archivo* archivo);
void arch_print_info(t_archivo_info* info);
void arch_print_bloques(t_list* bloques_de_datos);
void arch_destroy(t_archivo* archivo);
t_bloque_de_datos* arch_buscar_bloque(t_archivo* archivo, int numero_bloque);


#endif /* ARCHIVOS_H_ */
