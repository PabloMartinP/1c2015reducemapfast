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

typedef struct {
	int id;
	char nombre[255];
	size_t tamanio;
	int directorio;
	bool disponible;
	int cant_bloques;
} t_archivo_info;

typedef struct {
	t_archivo_info* info;
	t_list* bloques_de_datos; //guardo t_archivo_bloque_con_copias
} t_archivo;

int arch_get_new_id();
void arch_agregar(t_archivo* archivo);
t_archivo_bloque_con_copias* bloque_de_datos_create();
t_archivo_info* arch_get_info(char* nombre, int dir_padre) ;
void arch_formatear();
t_archivo* arch_crear();
void arch_print(t_archivo* archivo);
void arch_print_info(t_archivo_info* info);
void arch_print_bloques(t_list* bloques_de_datos);

void arch_destroy(t_archivo* archivo);
t_archivo_bloque_con_copias* arch_buscar_parte(t_archivo* archivo, int parte_numero);


/*
 * ***********************************
 */

t_archivo_bloque_con_copias* arch_buscar_parte(t_archivo* archivo, int parte_numero){
	t_archivo_bloque_con_copias* bloque = NULL;

	bool _buscar_bloque(t_archivo_bloque_con_copias* bloque){
		return bloque->parte_numero == parte_numero;
	}

	bloque = list_find(archivo->bloques_de_datos, (void*)_buscar_bloque);

	return bloque;
}

void arch_print(t_archivo* archivo){
	arch_print_info(archivo->info);
	arch_print_bloques(archivo->bloques_de_datos);
}

void arch_print_bloques(t_list* bloques_de_datos){

	void print_bloque_datos(t_archivo_bloque_con_copias* bloque_datos){
		printf(">> >> Bloque Nro: %d\n", bloque_datos->parte_numero);

		int i=1;
		void print_nodo_bloque(t_archivo_nodo_bloque* nodo_bloque){
			printf(" >> >> >> Copia %d: nodo_id: %d, bloque-nro: %d\n", i, nodo_bloque->base->id, nodo_bloque->numero_bloque);
			i++;
		}

		list_iterate(bloque_datos->nodosbloque, (void*)print_nodo_bloque);
	}

	list_iterate(bloques_de_datos, (void*)print_bloque_datos);
}

void arch_print_info(t_archivo_info* info){
	printf("Info del archivo '%s'\n", info->nombre);
	printf(">> id: %d\n", info->id);
	printf(">> Tamanio : %zd b, %.2f kb, %.2f mb\n", info->tamanio, bytes_to_kilobytes(info->tamanio), bytes_to_megabytes(info->tamanio));

	printf(">> Directorio padre: %d\n", info->directorio);
	printf(">> Disponible: %d\n", info->disponible);
	printf(">> Cantdidad de bloques: %d\n", info->cant_bloques);
}

t_archivo* arch_crear(){
	t_archivo* new = malloc(sizeof *new);
	new->bloques_de_datos = list_create();

	return new	;
}

void arch_destroy(t_archivo* archivo){
	//FREE_NULL(archivo->info);
	free(archivo->info);archivo->info = NULL;

	list_destroy_and_destroy_elements(archivo->bloques_de_datos, (void*)bloque_de_datos_destroy_no_free_base);
	//FREE_NULL(archivo);
	free(archivo);archivo = NULL;
}

int ARCHIVO_ID =1;
int arch_get_new_id(){
	return ARCHIVO_ID++;
}


t_archivo_info* arch_get_info(char* nombre, int dir_padre) {
	t_archivo_info* new = malloc(sizeof *new);
	//el id es siempre incremental
	new->id = 0;
	new->id = arch_get_new_id();
	new->disponible = true;
	//strcpy(new->nombre, basename(nombre));
	memset(new->nombre, ' ', sizeof(new->nombre));//aca hay que guardar solo el nombre, no el path completo
	strcpy(new->nombre, basename(nombre));
	//strcpy(new->nombre, nombre);
	new->tamanio = file_get_size(nombre);
	new->directorio = dir_padre;

	return new;
}


t_archivo_bloque_con_copias* bloque_de_datos_create() {
	t_archivo_bloque_con_copias* new = malloc(sizeof *new);
	new->nodosbloque = list_create();

	return new;
}


#endif /* ARCHIVOS_H_ */
