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
	//bool empezo_reduce_final;
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
bool job_obtener_nodo_con_todos_sus_mappers_terminados(t_list* mappers, t_nodo_base* nb);
bool map_termino(t_map* map);

char* generar_nombre_reduce(int job_id, int reduce_id);
char* generar_nombre_map(int job_id, int map_id);

t_map* marta_create_map(int id, int job_id, char* archivo, t_archivo_nodo_bloque* anb);
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
t_reduce* reduce_buscar(t_job* job, int reduce_id){
	bool _buscar_reduce(t_reduce* reduce){
		return reduce->info->id == reduce_id;
	}
	return list_find(job->reducers, (void*)_buscar_reduce);
}

t_map* marta_buscar_map(int job_id, int map_id){
	t_job* job = job_buscar(job_id);
	return map_buscar(job, map_id);
}
t_reduce* marta_buscar_reduce(int job_id, int map_id){
	t_job* job = job_buscar(job_id);
	return reduce_buscar(job, map_id);
}

int marta_marcar_map_como_fallido(int job_id, int map_id){
	t_map* map = marta_buscar_map(job_id, map_id);

	map->info->termino = false;
	return 0;
}


int marta_marcar_reduce_como_fallido(int job_id, int reduce_id){
	t_reduce* reduce = marta_buscar_reduce(job_id, reduce_id);

	reduce->info->termino = false;
	return 0;
}

bool terminaron_todos_los_mappers(t_list* mappers){
	bool _termino(t_map* map){
		return map->info->termino;
	}
	return list_all_satisfy(mappers, (void*)_termino);
}

bool terminaron_todos_los_reducers(t_list* reducers){
	bool _termino(t_reduce* reduce){
		return reduce->info->termino;
	}
	return list_all_satisfy(reducers, (void*)_termino);
}

t_nodo_base* job_obtener_nodo_para_reduce_final_combiner(t_job* job){
	t_reduce* reduce = NULL;
	t_nodo_base* nb = NULL;


	int cant_archivos1 = 0, cant_archivos2=0;
	bool _ordenar_por_cant_archivos_locales(t_reduce* red1, t_reduce* red2){
		bool _contar_archivos1(t_map* map){
			return nodo_base_igual_a(*(map->archivo_nodo_bloque->base), *(red1->nodo_base_destino));
		}
		cant_archivos1 = list_count_satisfying(job->mappers, (void*)_contar_archivos1);

		bool _contar_archivos2(t_map* map){
			return nodo_base_igual_a(*(map->archivo_nodo_bloque->base), *(red2->nodo_base_destino));
		}
		cant_archivos2 = list_count_satisfying(job->mappers, (void*)_contar_archivos2);

		return cant_archivos1 < cant_archivos2;
	}
	list_sort(job->reducers, (void*)_ordenar_por_cant_archivos_locales);

	//agarro el primero, el que tiene mas archivos locales
	reduce = list_get(job->reducers, 0);

	nb = reduce->nodo_base_destino;

	return nb;
}

int job_terminaron_todos_los_map_y_reduce(t_job* job){
	return terminaron_todos_los_mappers(job->mappers) && terminaron_todos_los_reducers(job->reducers);
}

bool map_termino(t_map* map){
	return map->info->termino;
}

bool job_obtener_nodo_con_todos_sus_mappers_terminados(t_list* mappers, t_nodo_base* nb){
	bool rs = false;

	//filtro los nodos iguales a mi
	bool _mismo_nodo(t_map* mapp){
		return nodo_base_igual_a(*(mapp->archivo_nodo_bloque->base), *(nb));
	}
	t_list* mappers_nodo = list_filter(mappers, (void*)_mismo_nodo);

	//si un nodo no termino todos sus mappers, no puedo lanzar el reduce de archivlos locales para con combiner
	rs = list_all_satisfy(mappers_nodo, (void*) map_termino);

	//list_destroy(mappers_nodo);

	return rs;
}



int marta_marcar_reduce_como_terminado(int job_id, int reduce_id){
	t_reduce* reduce = marta_buscar_reduce(job_id, reduce_id);

	reduce->info->termino = true;
	return 0;
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

	//new->empezo_reduce_final = false;

	return new;
}

t_map* marta_create_map(int id, int job_id, char* archivo, t_archivo_nodo_bloque* anb){
	t_map* new = map_create(id,job_id, archivo);
	new->archivo_nodo_bloque = anb;
	//new->info->nodo_base = nodo_base_new(ne->nodo->base->id, ne->nodo->base->red.ip,ne->nodo->base->red.puerto);
	new->info->id = id;
	new->info->job_id = job_id;
	return new;
}


#endif /* MARTA_H_ */

