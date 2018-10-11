-- Motor: PostgreSQL 10.4

----------------------------------------------------------------------------------------
-- Tablas del trabajo práctico N°1

/*
DROP TABLE IF EXISTS bufferpool;
DROP DATABASE IF EXISTS TP1;
*/

CREATE DATABASE TP1;

CREATE TABLE bufferpool
(
  nro_frame INT,
  free BOOLEAN,
  dirty BOOLEAN,
  nro_disk_page INT,
  last_touch TIMESTAMP
);

INSERT INTO bufferpool (nro_frame, free, dirty, nro_disk_page, last_touch)
VALUES	(1,FALSE,FALSE,NULL,NULL),
        (2,FALSE,FALSE,NULL,NULL),
        (3,FALSE,FALSE,NULL,NULL),
        (4,FALSE,FALSE,NULL,NULL);

SELECT * FROM bufferpool;

CREATE TABLE param_parametros
(
  param_codigo VARCHAR(30),
  param_valor1 INT
)

INSERT INTO param_parametros (param_codigo, param_valor1)
VALUES	('ULTIMO_NRO_PAGINA',null),
	('CANT_SOLIC_SECUENCIALES',0),
        ('CANT_SOLIC_SECUENCIALES_MAX',5);
	
SELECT * FROM param_parametros;

----------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS pick_frame_MRU();

CREATE OR REPLACE FUNCTION pick_frame_MRU()
	RETURNS INT AS
$BODY$
DECLARE
	resultado INTEGER;
BEGIN

	SELECT nro_frame
	INTO resultado
	FROM bufferpool
	ORDER BY last_touch DESC
	LIMIT 1 OFFSET 0;

	RETURN resultado;

END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

-- select pick_frame_MRU();

----------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS pick_frame_LRU();

CREATE OR REPLACE FUNCTION pick_frame_LRU()
	RETURNS INT AS
$BODY$
DECLARE
	resultado INTEGER;
BEGIN

	SELECT nro_frame
	INTO resultado
	FROM bufferpool
	ORDER BY last_touch ASC
	LIMIT 1 OFFSET 0;

	RETURN resultado;

END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

-- select pick_frame_LRU();
----------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS pick_frame_139();

CREATE OR REPLACE FUNCTION pick_frame_139()
	RETURNS INT AS
$BODY$
DECLARE
	ultNroPaginaSolicitado INTEGER;
	nSecuencial INTEGER;
	nSecuencialMax INTEGER;
	resultado INTEGER;
BEGIN

/*
Debe retornar el numero de frame que se debe desalojar segun LRU, pero antes de cada solicitud se debe verificar
si las ultimas N solicitudes fueron secuenciales (nros de pagina contiguos). Si hubo N secuenciales, debe retornar
el numero de frame segun MRU, y poner en cero el contador de secuenciales. N es un porcentaje de la cantidad de 
buffers en el pool (por ejemplo N=50%)
*/
	SELECT param_valor1
	INTO nSecuencialMax
	FROM param_parametros
	WHERE param_codigo = 'ULTIMO_NRO_PAGINA';

	SELECT param_valor1
	INTO nSecuencial
	FROM param_parametros
	WHERE param_codigo = 'CANT_SOLIC_SECUENCIALES';

	SELECT param_valor1
	INTO nSecuencialMax
	FROM param_parametros
	WHERE param_codigo = 'CANT_SOLIC_SECUENCIALES_MAX';

	//TODO...

	SELECT *
	INTO resultado
	FROM pick_frame_LRU();	
	
	RETURN resultado;

END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

-- select pick_frame_139();
----------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION get_disk_page (nro_page INTEGER)
  RETURNS INTEGER AS
$BODY$
DECLARE
  resultado INTEGER;
  pFrame INTEGER;
  
BEGIN

	RAISE NOTICE 'Se invoca get_disk_page(%)', _nro_page;

	IF EXISTS (SELECT FROM bufferpool WHERE nro_disk_page = nro_page) 
	THEN
	  SELECT nro_frame
	  INTO resultado
	  FROM bufferpool
	  WHERE nro_disk_page = nro_page;
	  
	  RAISE NOTICE 'Acceso a bufferpool. Frame %', resultado;
	ELSE
	
	  -- Busco un frame libre
	  SELECT nro_frame
	  INTO resultado
	  FROM bufferpool 
	  WHERE free = TRUE
	  ORDER BY last_touch
	  LIMIT 1;
	  
	  IF(resultado IS NULL)
	  THEN
	    -- Si no hay frame libre, desalojo segun algoritmo
			SELECT *
			INTO pFrame
			FROM pick_frame_LRU();

	    -- Si dirty = true, hay que hacer un update en disco (write_pag_to_disk())
	    -- antes de pisar el bloque 
	    RAISE NOTICE 'Acceso a disco con reemplazo. Frame %', resultado;
	  ELSE
	    RAISE NOTICE 'Acceso a disco sin reemplazo. Frame %', resultado;
	  END IF;
	  
	  UPDATE bufferpool
	  SET free = FALSE,
	    last_touch = current_timestamp
	  WHERE nro_frame = resultado;
	  
	END IF;

	RETURN resultado;

	EXCEPTION
	WHEN OTHERS THEN
		RAISE NOTICE 'Ocurrió un error general en get_disk_page() ';
		RAISE NOTICE '% %', SQLERRM, SQLSTATE;
	RETURN NULL;

END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

-- select get_disk_page(1);

----------------------------------------------------------------------------------------
