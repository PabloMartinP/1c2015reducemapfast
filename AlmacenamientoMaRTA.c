#include <stdio.h>
#include <stdlib.h>

ObtenerLista();
OrdenarEInsertar();
OrdenarPorEstado();
InsertarEnLista();
OrdenarListaPorEspacio();
BloquesAUsar();
CrearMatriz();
CargarNodoUno();
CargarDatos();
CargarMatriz();
CargarNodo();


int main(void) {

	int Cant, Copias, NodosCargados, Resto, i, j, Copia;

	LDN = ObtenerLista(ListaDeNodos);

	Cant = BloquesAUsar(Archivo);

	CrearMatriz(Cant, Copias, Matriz);

	NodosCargados = CargarNodoUno(LDN, Cant, Matriz);

	Resto = Cant - NodosCargados;

	for ( i = 0 ; (i = (Resto - 1)) ; i++){
		Copia = 0;
		CargarNodo(LDN, i, Copia ,Matriz);}

	for ( Copia = 1 ; (Copia = (Copias - 1)) ; Copia++){
		for ( j = 0 ; (j = (Cant - 1)) ; j++){
			CargarNodo(LDN, i, Copia ,Matriz);}
	}

	return EXIT_SUCCESS;
}





/*t*list*/ ObtenerLista(/*t*list*/ ListaDeNodos){
	/* t*list Ptr*/;
	Ptr = ListaDeNodos;

	while (Ptr =! NULL){
		OrdenarEInsertar(Ptr, ListaNueva);
		Ptr = Ptr -> sgte;
	}

	OrdenarListaPorEspacio( ListaNueva);
	return ListaNueva;
}

/* t*list*/ OrdenarEInsertar(/* t*list*/ PunteroALista, /* t*list*/ ListaNueva){
	int Contador = 0;
	int i;

	ListaNueva = NULL;
	PtrB = Ptr.info.Bloque;
	PtrN = Ptr.info.Nodo;

	OrdenarPorEstado(PrtB);

	while ((PtrB =! NULL) && ((PtrB.info.Estado) =! False))){
		Contador ++;
		PtrB = PtrB -> sgte;
	}

	for ( i = 0 ; ( i = (Contador - 1)) ; i++){
		Vec [i].IdBloque = Ptrb.info.Bloque;
	}

	InsertarEnLista(Vec, PtrN, ListaNueva, Contador);

	return ListaNueva;
}

/* t*lista*/ OrdenarPorEstado(/* t*lista*/ PunteroAListaDeBloque){

}

/* t*lista*/ InsertarEnLista (Vec, /* t*lista*/ PuntorDeListaANodo, /* t*lista*/ ListaNueva, Contador){

}

/* t*lista*/ OrdenarListaPorEspacio(/* t*lista*/ ListaNueva){

}

int BloquesAUsar(file* Archivo){
	int Tamanio, Cant;
	const Partes = 20;

	Tamanio = sizeof(Archivo);
	Cant = Tamanio / Partes;

	return Cant;
}

/*TypeMatriz*/ CrearMatriz (int Cant, Copias, /*Matriz */ Matriz){

}

int CargarNodoUno(/* t*lista*/ LDN,int Cant,/*Matriz*/ Matriz){
	int PrimerosBloques, Copia, Faltante, EnNodo1;

	PrimerosBloques = (Cant * 3) div 4;
	Copia = 0;
	Ptr = LDN;

	if (PrimerosBloques <= Ptr.info.CantB){
		for ( int i = 0 ; ( i = (Cant - 1)) ; i++){
			CargarDatos();
			CargarMatriz(Ptr, Copia, Matriz, i);
		}
	}else{
		Faltante = PrimerosBloques - Ptr.info.CantB;
		EnNodo1 = Ptr.info.CantB;

		for ( int i = 0 ; ( i = (EnNodo1 - 1)) ; i++){
					CargarDatos();
					CargarMatriz(Ptr, Copia, Matriz, i);
				}

		Ptr = Ptr -> sgte;

		for ( int j = 0 ; ( j = (Faltante - 1)) ; j++){
					CargarDatos();
					CargarMatriz(Ptr, Copia, Matriz, j);
				}
		}

	return PrimerosBloques;
}

/*Type Matriz*/ CargarMatriz (int Copia, i, /* t*list*/ PunteroALista){
	Matriz [Copia] [i] = Ptr.info.IdNodo;
}

int CargarNodo (int Copia, Var, /* t*lista*/ LDN,/*Matriz*/ Matriz){

	NLDN = InvertirOrden(LDN);				//Para obtener la lista de nodos más cargados

	CargarBloque(NLDN, Matriz);

	CargarMatriz(Copia, i, Ptr, Matriz);

	return 0;
}

int CargarBloque (/* t*lista*/ Lista,/*Matriz*/ Matriz){
	int Contador;
	Bool Termino;
	Ptr = Lista;

	do while Termino = False {
		if Ptr.info.IdNodo =! Matriz [Copia] [i-1]{
			while ((Ptr.info.IdNodo =! Matriz [J][i]) && Ptr.info.IdNodo =! Matriz [Copia] [i-1]){
				J++}
			if (Ptr.info.IdNodo =! Matriz [Copia] [i-1]){
				CargarDatos();
				Termino = True;

			}else{
				Ptr = Ptr -> sgte;
				Termino = False;
			}

		}else{
			Pter = Ptr -> sgte;
			Termino = False;
		}
	}
	return 0;
}
