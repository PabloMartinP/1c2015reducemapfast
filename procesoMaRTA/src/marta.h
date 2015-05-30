/*
 * marta.h
 *
 *  Created on: 27/5/2015
 *      Author: utnso
 */

#ifndef MARTA_H_
#define MARTA_H_

#include <nodo.h>

typedef struct{
	int id;
	char* ip;//para conectarme con el nodo
	int puerto;//para conectarme con el nodo
	char* resultado;//el nombre del archivo ya mapeado(solo el nombre porque siempre lo va buscar en el tmp del nodo)
	bool termino;//para saber si termino
}t_mapreduce;

typedef struct{
	t_mapreduce info;
	int n_bloque;//para saber que bloque tengo que aplicarle el map
}t_map;

typedef struct{

	t_mapreduce info;
}t_reduce;

typedef struct{
	char* nombre;
	t_list* bloque_de_datos;//guardo t_archivo_nodo_bloque
}t_archivo;
typedef struct {
	int id;
	t_list* archivos;//lista de archivos a procesar
	bool combiner;
	char* resultado;//el nombre del archivo resultado final
}t_job;

typedef struct {
	t_archivo_nodo_bloque* nodo;
	bool empezo;
	bool aplicando_map;
	bool aplicando_reduce;
}t_nodo_estado;

typedef struct {
	t_list* jobs;//guarda estructuras t_job
	t_list* nodos;
}t_MaRTA;

//a medida que se asignan nuevos jobs voy sumando 1
int JOB_ID = 0;
int JOB_MAP_ID=0;
int JOB_REDUCE_ID=0;

t_MaRTA marta;

t_job* marta_create_job(char* resultado, bool combiner);
int marta_create();
t_archivo* marta_create_archivo(char* nombre);
t_archivo_nodo_bloque*  marta_create_nodo_bloque(char* ip, int puerto, int numero_bloque, int nodo_id);
t_nodo_estado* marta_create_nodo_estado(t_archivo_nodo_bloque* cnb);

int marta_create(){
	marta.jobs = list_create();
	marta.nodos = list_create();
	return 0;
}

t_nodo_estado* marta_create_nodo_estado(t_archivo_nodo_bloque* cnb){
	t_nodo_estado* new = malloc(sizeof*new);

	new->nodo= cnb;

	new->empezo = false;
	new->aplicando_map = false;
	new->aplicando_reduce = false;
	return new;
}


t_archivo_nodo_bloque*  marta_create_nodo_bloque(char* ip, int puerto, int numero_bloque, int nodo_id){
	t_archivo_nodo_bloque* new = NULL;
	new = archivo_nodo_bloque_new(ip, puerto, numero_bloque, nodo_id);
	return new;
}

t_archivo* marta_create_archivo(char* nombre){
	t_archivo* new = malloc(sizeof*new);
	new->nombre = malloc(strlen(nombre)+1);
	strcpy(new->nombre, nombre);

	new->bloque_de_datos = list_create();

	return new;
}

t_job* marta_create_job(char* resultado, bool combiner){
	t_job* new = malloc(sizeof*new);

	new->archivos = list_create();

	new->resultado = malloc(strlen(resultado)+1);
	strcpy(new->resultado, resultado);

	new->combiner = combiner;
	new->id = JOB_ID++;//le asigno un nuevo id

	return new;
}

t_map* marta_create_map(char* archivo){
	t_map* new = malloc(sizeof*new);

	new->info.termino = false;


	new->info.id  = JOB_MAP_ID++;
	return new;
}


#endif /* MARTA_H_ */
