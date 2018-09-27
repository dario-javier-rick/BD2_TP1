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
        (2,FALSE,FALSE,NULL,NULL),
        (2,FALSE,FALSE,NULL,NULL);


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
	    pick_frame_LRU();
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
