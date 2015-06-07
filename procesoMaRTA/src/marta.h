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
	char* nombre;
	t_list* bloque_de_datos;//guardo t_archivo_nodo_bloque
}t_archivo_job;
typedef struct {
	int id;
	t_list* archivos;//lista de archivos a procesar
	bool combiner;
	char* resultado;//el nombre del archivo resultado final
	t_list* mappers;//guarda t_maps
	t_list* reducers; //guarda t_reduce
}t_job;


typedef struct {
	t_list* jobs;//guarda estructuras t_job
	//t_list* nodos_mappers;//list of t_nodo_estado_map
	//t_list* nodos_reducers;
}t_MaRTA;

//a medida que se asignan nuevos jobs voy sumando 1
int JOB_ID = 0;
int JOB_MAP_ID=0;
int JOB_REDUCE_ID=0;
t_MaRTA marta;

t_job* marta_create_job(char* resultado, bool combiner);
int marta_create();
t_archivo_job* marta_create_archivo(char* nombre);
t_archivo_nodo_bloque*  marta_create_nodo_bloque(char* ip, int puerto, int numero_bloque, int nodo_id);
void archivo_nodo_bloque_destroy(t_archivo_nodo_bloque* anb);
//t_nodo_estado_map* marta_create_nodo_estado(t_archivo_nodo_bloque* cnb);
int marta_marcar_map_como_terminado(int job_id, int map_id);
int marta_marcar_map_como_fallido(int job_id, int map_id);

t_map* map_buscar(t_job* job, int map_id);
t_map* marta_buscar_map(int job_id, int map_id);
t_nodo_base* job_obtener_nodo_con_todos_sus_mappers_terminados(t_list* mappers);

char* generar_nombre_reduce(int job_id, int reduce_id);
char* generar_nombre_map(int job_id, int map_id);

t_map* marta_create_map(int id, char* archivo, t_archivo_nodo_bloque* anb);
char* generar_nombre_job(int job_id, int mapreduce_id, char*map_o_reduce);
//void nodo_estado_map_destroy(t_nodo_estado_map* ne);


/*
 *
 */
char* generar_nombre_map(int job_id, int map_id){
	return generar_nombre_job(job_id, map_id, "map");
}

/*
void nodo_estado_map_destroy(t_nodo_estado_map* ne){

	//archivo_nodo_bloque_destroy(ne->nodo);
	//FREE_NULL(ne->nodo); //elne->nodo ya se libero en el archivo_destroy
	free(ne);ne = NULL;
}*/

char* generar_nombre_job(int job_id, int mapreduce_id, char*map_o_reduce){
	char* file_map1 = string_new();
	string_append(&file_map1, "job_");

	char str[3];

	sprintf(str, "%d", job_id);
	string_append(&file_map1, str);
	string_append(&file_map1, "_");

	string_append(&file_map1, map_o_reduce);
	string_append(&file_map1, "_");
	sprintf(str, "%d", mapreduce_id);
	string_append(&file_map1, str);
	string_append(&file_map1, "_");

	char* timenow = temporal_get_string_time();
	string_append(&file_map1, timenow);
	free(timenow);
	string_append(&file_map1, ".txt");
	return file_map1;
}

char* generar_nombre_reduce(int job_id, int reduce_id){
	return generar_nombre_job(job_id, reduce_id, "reduce");
}



t_job* job_buscar(int job_id){
	bool _buscar_job(t_job* job){
			return job->id == job_id;
		}
		return list_find(marta.jobs, (void*)_buscar_job);
}


t_map* map_buscar(t_job* job, int map_id){
	bool _buscar_map(t_map* map){
		return map->info->id == map_id;
	}
	return list_find(job->mappers, (void*)_buscar_map);
}
t_map* marta_buscar_map(int job_id, int map_id){
	t_job* job = job_buscar(job_id);
	return map_buscar(job, map_id);
}


int marta_marcar_map_como_fallido(int job_id, int map_id){
	t_map* map = marta_buscar_map(job_id, map_id);

	map->info->termino = false;
	return 0;
}




t_nodo_base* job_obtener_nodo_con_todos_sus_mappers_terminados(t_list* mappers){
	t_nodo_base* nb;


	void _nodo_termino_todos_sus_mappers(t_map* map){
		nb = map->archivo_nodo_bloque->base;

		bool _nodo_termino(t_map* map_nodo){
			return map_nodo->info->termino && nodo_base_igual_a(*(map_nodo->archivo_nodo_bloque->base), *(map->archivo_nodo_bloque->base));
			//return map_nodo->info->termino && nodo_base_igual_a(*(map_nodo->archivo_nodo_bloque->base), *(map->archivo_nodo_base->nodo_base));
		}
		//si un nodo no termino todos sus mappers, no puedo lanzar el reduce de archivlos locales para con combiner
		if(!list_all_satisfy(mappers, (void*)_nodo_termino)){
			nb = NULL;
		}
	}
	list_iterate(mappers, (void*)_nodo_termino_todos_sus_mappers);

	return nb;
}

int marta_marcar_map_como_terminado(int job_id, int map_id){
	t_map* map = marta_buscar_map(job_id, map_id);

	map->info->termino = true;
	return 0;
}

int marta_create(){
	marta.jobs = list_create();
	//marta.nodos_mappers = list_create();
	return 0;
}


void marta_destroy(){
	list_destroy(marta.jobs);

	//list_destroy_and_destroy_elements(marta.nodos_mappers, (void*)nodo_estado_map_destroy);
}

void map_destroy(t_map* map){
	FREE_NULL(map->info->resultado);
	//FREE_NULL(map->info->nodo_base);
	FREE_NULL(map->info);

	FREE_NULL(map);
}

void marta_map_destroy(int job_id, int map_id){
	t_job* job = job_buscar(job_id);

	bool _buscar_map(t_map* map){
		return map->info->id == map_id;
	}

	list_remove_and_destroy_by_condition(job->mappers, (void*)_buscar_map, (void*)map_destroy);

}

void job_destroy(t_job* job){

	//elimino los bloques de datos del archivo,
	//los nombres los dejo por si falla y volver a crear los bloques de datos consultando al fs
	//asi tengo en cuenta si se conecto un nodo nuevo
	void _archivo_destroy(t_archivo_job* archivo){
		FREE_NULL(archivo->nombre);
		list_destroy_and_destroy_elements(archivo->bloque_de_datos, (void*) bloque_de_datos_destroy_free_base);

		FREE_NULL(archivo);
	}
	list_destroy_and_destroy_elements(job->archivos,(void*)_archivo_destroy);


	FREE_NULL(job->resultado);
	list_destroy(job->mappers);//en teoria no hay ninguno
	FREE_NULL(job);
}

void marta_job_destroy(int id){
 	//t_job* job = job_buscar(id);

 	bool _buscar_job(t_job* job){
 		return job->id == id;
 	}

 	list_remove_and_destroy_by_condition(marta.jobs, (void*)_buscar_job, (void*)job_destroy);

}


t_archivo_nodo_bloque*  marta_create_nodo_bloque(char* ip, int puerto, int numero_bloque, int nodo_id){
	t_archivo_nodo_bloque* new = NULL;
	new = archivo_nodo_bloque_new(ip, puerto, numero_bloque, nodo_id);
	return new;
}

t_archivo_job* marta_create_archivo(char* nombre){
	t_archivo_job* new = malloc(sizeof*new);
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

	new->mappers = list_create();
	new->reducers = list_create();

	return new;
}

t_map* marta_create_map(int id, char* archivo, t_archivo_nodo_bloque* anb){
	t_map* new = map_create(id, archivo);
	new->archivo_nodo_bloque = anb;
	//new->info->nodo_base = nodo_base_new(ne->nodo->base->id, ne->nodo->base->red.ip,ne->nodo->base->red.puerto);

	return new;
}


#endif /* MARTA_H_ */

