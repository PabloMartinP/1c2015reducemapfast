/*
 * nodo.c
 *
 *  Created on: 29/4/2015
 *      Author: utnso
 */
#include "nodo.h"




void map_free_all(t_map* map){
	//FREE_NULL(map->info->nodo_base);
	FREE_NULL(map->info->resultado);
	FREE_NULL(map->info);
	archivo_nodo_bloque_destroy_free_base(map->archivo_nodo_bloque);
	FREE_NULL(map);

}

void mapreduce_destroy(t_mapreduce* mr){
	FREE_NULL(mr->resultado);
	FREE_NULL(mr);
}

void reduce_free(t_reduce* reduce){

	mapreduce_destroy(reduce->info);

	FREE_NULL(reduce->nodo_base_destino);


	list_destroy_and_destroy_elements(reduce->nodos_archivo, (void*)nodo_archivo_destroy);
	FREE_NULL(reduce);
}


bool nodo_esta_activo(t_nodo_base* nb){
	bool on ;
	//obtengo un socket cliente para ver si responde
	//int fd = client_socket(nodo->ip, nodo->puerto);
	int fd = client_socket(nb->red.ip, nb->red.puerto);

	if(fd<0){
		return false;
	}

	//le mando handshake a ver si me responde el HOLA
	t_msg* msg = argv_message(NODO_HOLA, 0);
	enviar_mensaje(fd, msg);
	destroy_message(msg);
	msg = recibir_mensaje(fd);

	//si responde NODO_HOLA esta activo
	if (msg == NULL) {
		on = false;
	} else {
		on = msg->header.id == NODO_HOLA;
		destroy_message(msg);
	}
	close(fd);
	//enviar_mensaje_nodo_close(fd);


	return on;
}



/*
 * envio un mensaje de desconexion
 */
void nodo_mensaje_desconexion(t_nodo* nodo){

	printf("Enviar msg de desconexion al nodo %d, ip:%s:%d\n", nodo->base->id, nodo->base->red.ip, nodo->base->red.puerto);
	//me conecto al nodo
	int fd = client_socket(nodo->base->red.ip, nodo->base->red.puerto);

	t_msg* msg = string_message(NODO_CHAU, "",0);

	enviar_mensaje(fd, msg);
	destroy_message(msg);

}

t_archivo_bloque_con_copias* bloque_de_datos_crear(){
	t_archivo_bloque_con_copias* new = malloc(sizeof*new);
	new->nodosbloque = list_create();
	return new;
}

bool bloque_esta_usado(t_bloque* bloque){
	return !bloque->libre;
}
bool bloque_esta_libre(t_bloque* bloque){
	return bloque->libre;
}

bool bloque_esta_para_copiar(t_bloque* bloque){
	return !bloque->requerido_para_copia;
}

/*
 * devuelvo un bloque libre cualquiera
 */
t_bloque* nodo_get_bloque_libre(t_nodo* nodo){
	t_bloque* b;

	b = list_find(nodo->bloques, (void*)bloque_esta_libre);

	return b;
}

void archivo_nodo_bloque_destroy_no_free_base(t_archivo_nodo_bloque* anb){
	//FREE_NULL(anb->base);
	FREE_NULL(anb);
}


void archivo_nodo_bloque_destroy_free_base(t_archivo_nodo_bloque* anb){
	FREE_NULL(anb->base);
	FREE_NULL(anb);
}

void bloque_de_datos_destroy_free_base(t_archivo_bloque_con_copias* bloque_de_datos){
	list_destroy_and_destroy_elements(bloque_de_datos->nodosbloque,(void*) archivo_nodo_bloque_destroy_free_base);
	FREE_NULL(bloque_de_datos);

}

void bloque_de_datos_destroy_no_free_base(t_archivo_bloque_con_copias* bloque_de_datos){
	list_destroy_and_destroy_elements(bloque_de_datos->nodosbloque,(void*) archivo_nodo_bloque_destroy_no_free_base);
	FREE_NULL(bloque_de_datos);

}


t_bloque* nodo_get_bloque_para_copiar(t_nodo* nodo){
	t_bloque* b;

	b = list_find(nodo->bloques, (void*)bloque_esta_para_copiar);
	if(b == NULL){
		printf("El nodo %d no tiene bloques libres\n", nodo->base->id);
		return NULL;
	}
	b->requerido_para_copia = true;

	return b;
}


int nodo_cant_bloques_requerido_para_copia(t_nodo* nodo){
	return list_count_satisfying((void*)nodo->bloques, (void*)bloque_esta_para_copiar );
}

int nodo_cant_bloques_libres(t_nodo* nodo){
	return list_count_satisfying((void*)nodo->bloques, (void*)bloque_esta_libre );
}
int nodo_cant_bloques_usados(t_nodo* nodo){
	return list_count_satisfying((void*)nodo->bloques, (void*)bloque_esta_usado );
}
int nodo_cant_bloques(t_nodo* nodo){
	return list_size((void*)nodo->bloques);
}

void print_nodo(t_nodo* nodo){
	printf("Id: %d, Nuevo: %d, Ip: %s, Puerto: %d\n", nodo->base->id, nodo->esNuevo, nodo->base->red.ip, nodo->base->red.puerto);
}

void nodo_marcar_como_libre_total(t_nodo* nodo){
	void _marcar_como_libre(t_bloque* bloque){
		bloque->libre = true;
		bloque->requerido_para_copia = false;
	}

	list_iterate(nodo->bloques, (void*)_marcar_como_libre);
}

char* nodo_isNew(t_nodo* nodo) {
	return (nodo->esNuevo) ? "Nuevo" : "Viejo";
}

void nodo_print_info(t_nodo* nodo){
	printf("id: %d, %s:%d\n", nodo->base->id, nodo->base->red.ip, nodo->base->red.puerto);
	printf("cant_bloques: %d, cant_bloques_libres: %d, cant_bloques_usados: %d\n", nodo_cant_bloques(nodo), nodo_cant_bloques_libres(nodo), nodo_cant_bloques_usados(nodo));
}

void nodo_destroy(t_nodo* nodo) {
	list_destroy_and_destroy_elements(nodo->bloques, free);
	free(nodo->base);nodo->base = NULL;

	free(nodo);
	nodo = NULL;
	//printf("%s", nodo->ip);
}



void nodo_marcar_bloque_como_usado(t_nodo* nodo, int n_bloque){
	t_bloque* bloque = NULL;
	bloque = nodo_buscar_bloque(nodo, n_bloque);
	bloque->libre = false;
	bloque->requerido_para_copia = true;//se usa al copiar del fs local al mdfs
}
void nodo_marcar_bloque_como_libre(t_nodo* nodo, int n_bloque){
	t_bloque* bloque = NULL;
	bloque = nodo_buscar_bloque(nodo, n_bloque);
	bloque->libre = true;
	bloque->requerido_para_copia = false;//se usa al copiar del fs local al mdfs
}

t_bloque* nodo_buscar_bloque(t_nodo* nodo, int n_bloque){
	bool _buscar_bloque(t_bloque* bloque){
		return bloque->posicion == n_bloque;
	}
	return list_find(nodo->bloques, (void*)_buscar_bloque);
}



void nodo_archivo_destroy(t_nodo_archivo* na){
	FREE_NULL(na->nodo_base);
	FREE_NULL(na);
}


t_nodo* nodo_new(char* ip, int puerto, bool isNew, int cant_bloques, int id) {
	t_nodo* new = malloc(sizeof *new);
	new->base = nodo_base_new(id, ip, puerto);

	//le asigno un nuevo id solo si es nuevo
	//nodo_set_ip(new, ip);

	new->cant_bloques = cant_bloques;
	new->esNuevo = isNew;
	new->conectado = false;

	new->bloques = list_create();

	t_bloque* bloque;
	int i;
	for (i = 0; i < cant_bloques; i++) {
		bloque = malloc(sizeof *bloque);
		bloque->posicion = i;
		bloque->libre = true;
		bloque->requerido_para_copia = false;

		list_add(new->bloques, bloque);
	}


	return new;
}
/*
bool nodo_base_igual_a(t_nodo_base* nb, t_nodo_base* otro_nb){
	return nb->id == otro_nb->id && nb->red.puerto == otro_nb->red.puerto && strcmp(nb->red.ip, otro_nb->red.ip) ==0;
}*/

bool nodo_base_igual_a(t_nodo_base nb, t_nodo_base otro_nb){
	return nb.id == otro_nb.id && nb.red.puerto == otro_nb.red.puerto && strcmp(nb.red.ip, otro_nb.red.ip) ==0;
}

