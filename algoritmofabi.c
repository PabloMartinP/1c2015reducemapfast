#include <util.h>
#include <stdio.h>
#include <stdlib.h>
#include <commons/collections/list.h>
#include <stdbool.h>


typedef struct{
	t_list* matriz [] [];
}t_matriz; //acomodar

typedef struct{
	int IdBloque;
}vec_ids;

t_list* ObtenerLista();
t_list* OrdenarEInsertar();
t_list* OrdenarPorEstado();
t_list* InsertarEnLista();
t_list* OrdenarListaPorEspacio();
int BloquesAUsar();
t_matriz* CrearMatriz();
int CargarNodoUno();
CargarDatos();
t_matriz* CargarMatriz();
CargarNodo();

const int Copias = 3; //cantidad de copias a copiar

int main(void) {

	int Cant, Copias, NodosCargados, Resto, i, j, Copia;
	t_list* LDN = ObtenerLista(ListaDeNodos);
	Cant = BloquesAUsar(Archivo);

	t_matriz* CrearMatriz(Cant, Copias, Matriz);

	NodosCargados = CargarNodoUno(LDN, Cant, Matriz);
	Resto = Cant - NodosCargados;

	for( i = 0 ; (i = (Resto-1)) ; i++){
		Copia = 0;
		CargarNodo(LDN, i, Copia ,Matriz);
	}
	for( Copia = 1 ; (Copia = (Copias - 1)) ; Copia++){
		for( j = 0 ; (j = (Cant - 1)) ; j++){
			CargarNodo(LDN, i, Copia ,Matriz);
		}
	}
	//liberar listas
	return EXIT_SUCCESS;
}


t_list* ObtenerLista(t_list* ListaDeNodos){
	t_list* Ptr, ListaNueva;
	list_add_all(Ptr,ListaDeNodos);

	while (Ptr != NULL){
		ListaNueva = OrdenarEInsertar(Ptr);
		Ptr = Ptr->sgte;
	}
	OrdenarListaPorEspacio(ListaNueva); //list_sort(ListaNueva);

	return ListaNueva;
}

t_list* OrdenarEInsertar(t_list* PunteroALista){
	int Contador=0;
	int i;
	t_list* ListaNueva = list_create();
	t_list* PtrB = PunteroALista->info->Bloque;
	t_list* PtrN = PunteroALista->info->Nodo;

	OrdenarPorEstado(PtrB); //list_sort(PtrB, list_true(PtrB));

	while((PtrB!=NULL) && (PtrB.info.Estado!=false)){
		Contador ++;
		PtrB = PtrB -> sgte;
	}

	vec_ids vec[Contador];

	for ( i = 0 ; ( i = (Contador-1)) ; i++){
		vec[i].IdBloque = PtrB.info.Bloque;
	}

	InsertarEnLista(vec, PtrN, ListaNueva, Contador);
	//list_add(ListaNueva)
	return ListaNueva;
}

///*t_list*/ OrdenarPorEstado(t_list* PunteroAListaDeBloque){

//}

///*t_list*/ InsertarEnLista (Vec, t_list* PunteroDeListaANodo, t_list* ListaNueva, Contador){

//}

/*/*t_list* OrdenarListaPorEspacio(t_list* ListaNueva){
//mas vacio a mas lleno
} */

int BloquesAUsar(FILE* Archivo){
	int Tamanio, Cant;
	const int Partes = 20;

	Tamanio = sizeof(Archivo);
	Cant = bytes_to_megabytes(Tamanio) / Partes;

	return Cant;
}

/*t_matriz*/ CrearMatriz (int Cant, Copias, /*t_matriz */ Matriz){

}

int CargarNodoUno(t_list* LDN, int Cant, t_matriz Matriz){
	int PrimerosBloques, Copia, Faltante, EnNodo1;
	Copia = 0;
	t_list* Ptr;
	list_add_all(Ptr,LDN);
	PrimerosBloques = div((Cant * 3), 4);

	if (PrimerosBloques <= Ptr.info.CantB){
		for ( int i = 0 ; ( i = (PrimerosBloques-1)) ; i++){
			CargarDatos(); // carga al nodo
			CargarMatriz(Ptr, Copia, Matriz, i);
		}
	}else{
		Faltante = PrimerosBloques - Ptr.info.CantB;
		EnNodo1 = Ptr.info.CantB;

		for( int i = 0 ; ( i = (EnNodo1 - 1)) ; i++){
					CargarDatos();
					CargarMatriz(Ptr, Copia, Matriz, i);
		}

		while(Faltante!=0){
			Ptr = Ptr->sgte;
			for ( int j = 0 ; ( j = (Faltante - 1)) ; j++){
				CargarDatos();
				CargarMatriz(Ptr, Copia, Matriz, j);
			}

		}
	}

	return PrimerosBloques;
}

/*t_matriz**/ CargarMatriz (int Copia,int i, t_list* PunteroALista){
	t_matriz* Matriz [Copia][i] = Ptr.info.IdNodo;
	return Matriz;
}

int CargarNodo (t_list* LDN, int Contador,int Copia, t_matriz* Matriz){

	t_list* NLDN = InvertirOrden(LDN);	//Para obtener la lista de nodos más cargados

	CargarBloque(NLDN, Matriz, Copia, Contador);

	Matriz = CargarMatriz(Copia, Contador, NLDN);

	return 0;
}

int CargarBloque (t_list* Lista,t_matriz Matriz, int Copia, int i){
	int Contador=0;
	bool Termino = false;
	t_list* Ptr;
	list_add_all(Ptr,Lista);

	do{
		if(Ptr.info.IdNodo != Matriz [Copia][i-1]){
			while ((Ptr.info.IdNodo != Matriz [Contador][i]) && (Ptr.info.IdNodo != Matriz [Copia] [i-1])){
				Contador++;
			}
			if(Ptr.info.IdNodo != Matriz [Copia][i-1]){
				CargarDatos();
				Termino = true;
			}else{
				Ptr = Ptr->sgte;
				Termino = false;
			}
		}else{
			Ptr = Ptr->sgte;
			Termino = false;
		}
	}while(Termino == false);

	return 0;
}
