Proyecto TP1

Usando la siguiente tabla de postgres

bufferpool( 	nro_frame 		int,
		free			boolean,
		dirty			boolean,
		nro_disk_page	int,
		last_touch		timestamp
);

Se deben hacer los insert para inicializar la tabla buffer_pool con 4 buffers.
Implementar 4 funciones en postgres.

get_disk_page (nro_page): Debe retornar el nro_frame donde se encuentra la pagina de disco solicitada. Está funcion debe:

Implementar el algoritmo de lectura de disco o de buffer pool visto en clase.
Cuando la funcion debe elegir un frame para liberar, debe invocar a la funcion pick_frame_LRU o pick_frame_MRU o pick_frame139
La funcion (mediante la clausula RAISE) debe imprimir un mensaje indicando si leyo de disco o leyo del buffer pool la pagina solicitada, y si desalojo alguna pagina, cual fue.

pick_frame_LRU (): Debe retornar el numero de frame que se debe desalojar segun LRU

pick_frame_MRU (): Debe retornar el numero de frame que se debe desalojar segun MRU

pick_frame_139 (): Debe retornar el numero de frame que se debe desalojar segun LRU, pero antes de cada solicitud se debe verificar si las ultimas N solicitudes fueron secuenciales (nros de pagina contiguos). Si hubo N secuenciales, debe retornar el numero de frame segun MRU, y poner en cero el contador de secuenciales. N es un porcentaje de la cantidad de buffers en el pool (por ejemplo N=50%)

c) Se deben crear 3 trazas de paginas a leer de disco, y crear un script para ejecutar la funcion get_disk_page() varias veces con todas las trazas de paginas a leer de disco. Por ejemplo:

select get_disk_page (100);
select get_disk_page (101);
select get_disk_page (102);
select get_disk_page (103);
select get_disk_page (101);
select get_disk_page (102);
select get_disk_page (105);
select get_disk_page (102);
select get_disk_page (103);
select get_disk_page (106);
select get_disk_page (102);

Este punto se debe hacer usando MRU , LRU y 139 se deben comparar todos los resultados. En un caso, MRU debe ser mejor, en otro LRU debe ser mejor y en otro 139 debe ser mejor.
