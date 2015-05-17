/*
 * procesos.h
 *
 *  Created on: 17/5/2015
 *      Author: utnso
 */

#ifndef PROCESOS_H_
#define PROCESOS_H_

#include <util.h>
#include "config_job.h"


int conectar_con_marta();

int conectar_con_marta(){
	int res;
	//conecto con marta
	printf("conectando con marta\n");
	int fd ;
	if((fd= client_socket(JOB_IP_MARTA(), JOB_PUERTO_MARTA()))==-1){
		printf("No se pudo conectar con marta. MaRTA: %s:%d\n", JOB_IP_MARTA(), JOB_PUERTO_MARTA());
		handle_error("client_socket");
	}
	printf("Conectado\n");

	t_msg* msg;
	msg = string_message(JOB_HOLA, "hola soy un job",0);


	//envio el mensaje
	printf("enviando mensaje\n");
	res = enviar_mensaje(fd, msg);
	destroy_message(msg);
	if(res==-1){
		printf("no se pudo enviar mensaje a marta\n");
		handle_error("enviar_mensaje");
	}
	printf("mensaje enviado\n");

	printf("Esperando respuesta\n");
	//recibo la respuesta
	if((msg= recibir_mensaje(fd))==NULL){
		handle_error("recibir_mensaje");
	}
	printf("Mensaje recibido\n");
	//muestro el mensaje que me envio
	print_msg(msg);

	destroy_message(msg);

	return 0;
}



#endif /* PROCESOS_H_ */
