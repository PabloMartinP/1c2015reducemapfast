################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/config_nodo.c \
../src/procesoNodo.c 

OBJS += \
./src/config_nodo.o \
./src/procesoNodo.o 

C_DEPS += \
./src/config_nodo.d \
./src/procesoNodo.d 


# Each subdirectory must supply rules for building sources it contributes
src/%.o: ../src/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -I"/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/utiles" -O0 -g3 -Wall -c -fmessage-length=0  -D_FILE_OFFSET_BITS=64 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


