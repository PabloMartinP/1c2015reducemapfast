#include <stdlib.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>

#include "socket.h"

		//Mande un mensaje a un socket determinado
		int mandarMensaje(int unSocket, int8_t tipo, int tamanio, void *buffer) {

			Header header;
			int auxInt;
			//Que el tamanio lo mande
			void* bufferAux;

			header.type = tipo;
			header.payloadlength = tamanio;
			bufferAux=malloc(sizeof(Header)+tamanio);
			memcpy(bufferAux,&header,sizeof(Header));
			memcpy((bufferAux+(sizeof(Header))),buffer,tamanio);
//			if ((auxInt=send(unSocket, &header, sizeof(Header), 0)) >= 0){
			auxInt=send(unSocket, bufferAux,(sizeof(Header)+tamanio), 0);
			free(bufferAux);
			return auxInt;


//			}


		}

		//Recibe un mensaje del servidor - Version Lucas
		int recibirMensaje(int unSocket, void** buffer) {

			Header header;
			int auxInt;
			if((auxInt=recv(unSocket, &header, sizeof(Header), 0))>=0) {
				*buffer = malloc (header.payloadlength);
				if ((auxInt=recv(unSocket, *buffer, header.payloadlength, 0)) >= 0) {
					return  auxInt;
				}
			}
			return  auxInt;

		}
		//Recibe un mensaje del servidor - Version Lucas
		int recibirHeader(int unSocket, Header* header) {
			int auxInt;
				if((auxInt=recv(unSocket, header, sizeof(Header), 0))>=0) {
					return auxInt;
					}
					return auxInt;
				}

		int recibirData(int unSocket, Header header, void** buffer){
			int auxInt;
			*buffer = malloc (header.payloadlength);
					if ((auxInt=recv(unSocket, buffer, header.payloadlength, 0)) >= 0) {
						return auxInt;
					}
			return auxInt;
		}
		//Recibe un mensaje del servidor - Version SO
		int recv_variable(int socketReceptor, void* buffer) {

			Header header;
			int bytesRecibidos;

		// Primero: Recibir el header para saber cuando ocupa el payload.
			if (recv(socketReceptor, &header, sizeof(header), 0) <= 0)
				return -1;

		// Segundo: Alocar memoria suficiente para el payload.
			buffer = malloc(header.payloadlength);

		// Tercero: Recibir el payload.
			if((bytesRecibidos = recv(socketReceptor, buffer, header.payloadlength, 0)) < 0){
				free(buffer);
				return -1;
			}

			return bytesRecibidos;

		}

		int solicitarSocketAlSO() {

			int unSocket;
			int optval = 1;

			// Crear un socket:
			// AF_INET: Socket de internet IPv4
			// SOCK_STREAM: Orientado a la conexion, TCP
			// 0: Usar protocolo por defecto para AF_INET-SOCK_STREAM: Protocolo TCP/IPv4
			if ((unSocket = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
				perror("Error al crear socket");
				return EXIT_FAILURE;
			}

			// Hacer que el SO libere el puerto inmediatamente luego de cerrar el socket.
			setsockopt(unSocket, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));

			return unSocket;
		}

		struct sockaddr_in especificarSocketInfo(char* direccion, int puerto) {

			struct sockaddr_in socketInfo;

			socketInfo.sin_family = AF_INET;
			socketInfo.sin_addr.s_addr = inet_addr(direccion);
			socketInfo.sin_port = htons(puerto);

			return socketInfo;
		}

		//Crea un socket
		int quieroUnPutoSocketAndando(char* direccion, int puerto) {

			int unSocket = solicitarSocketAlSO();
			struct sockaddr_in socketInfo = especificarSocketInfo (direccion, puerto);

			printf("Conectando...\n");

			// Conectar el socket con la direccion 'socketInfo'.
			if (connect(unSocket, (struct sockaddr*) &socketInfo, sizeof(socketInfo))
					!= 0) {
				perror("Error al conectar socket");
				return 0;
			}

			printf("Conectado!\n");
			return unSocket;
		}

		int quieroUnPutoSocketDeEscucha(int puerto) {

			int socketEscucha = solicitarSocketAlSO();
			//Se pasa la dirección 0.0.0.0 porque es el equivalente en string de INADDR_ANY
			struct sockaddr_in socketInfo = especificarSocketInfo ("0.0.0.0", puerto);

			// Vincular el socket con una direccion de red almacenada en 'socketInfo'.
			if (bind(socketEscucha, (struct sockaddr*) &socketInfo, sizeof(socketInfo))
					!= 0) {

				perror("Error al bindear socket escucha");
				return EXIT_FAILURE;
			}

			return socketEscucha;
		}

		int aceptarNuevaConexion(int sock_fd)
		{
			struct sockaddr_in clientname;
			size_t size = sizeof clientname;

			int new_fd = accept(sock_fd, (struct sockaddr *) &clientname, (socklen_t *) &size);
			if (new_fd < 0) {
				perror("accept");
				return -1;
			}

			return new_fd;
		}




		//Size of each chunk of data received, try changing this
		#define CHUNK_SIZE 512
		/*
		    Receive data in multiple chunks by checking a non-blocking socket
		    Timeout in seconds
		*/
		int recv_timeout(int s , int timeout)
		{
		    int size_recv , total_size= 0;
		    struct timeval begin , now;
		    char chunk[CHUNK_SIZE];
		    double timediff;

		    //make socket non blocking
		    fcntl(s, F_SETFL, O_NONBLOCK);

		    //beginning time
		    gettimeofday(&begin , NULL);

		    while(1)
		    {
		        gettimeofday(&now , NULL);

		        //time elapsed in seconds
		        timediff = (now.tv_sec - begin.tv_sec) + 1e-6 * (now.tv_usec - begin.tv_usec);

		        //if you got some data, then break after timeout
		        if( total_size > 0 && timediff > timeout )
		        {
		            break;
		        }

		        //if you got no data at all, wait a little longer, twice the timeout
		        else if( timediff > timeout*2)
		        {
		            break;
		        }

		        memset(chunk ,0 , CHUNK_SIZE);  //clear the variable
		        if((size_recv =  recv(s , chunk , CHUNK_SIZE , 0) ) < 0)
		        {
		            //if nothing was received then we want to wait a little before trying again, 0.1 seconds
		            usleep(100000);
		        }
		        else
		        {
		            total_size += size_recv;
		            printf("%s" , chunk);
		            //reset beginning time
		            gettimeofday(&begin , NULL);
		        }
		    }

		    return total_size;
		}

		int sendAll(int s, char *buf, int *len) {
			int total = 0; // cuántos bytes hemos enviado
			int bytesleft = *len; // cuántos se han quedado pendientes
			int n;
			while (total < *len) {
				n = send(s, buf + total, bytesleft, 0);
				if (n == -1) {
					break;
				}
				total += n;
				bytesleft -= n;
			}
			*len = total; // devuelve aquí la cantidad enviada en realidad
			return n == -1 ? -1 : 0; // devuelve -1 si hay fallo, 0 en otro caso
		}
