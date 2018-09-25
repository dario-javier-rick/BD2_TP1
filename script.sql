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
  nro_disk_page	INT,
  last_touch TIMESTAMP
);

INSERT INTO bufferpool (nro_frame, free, dirty, nro_disk_page, last_touch)
VALUES	(1,FALSE,FALSE,1,NULL),
        (2,FALSE,FALSE,2,NULL),
        (2,FALSE,FALSE,3,NULL),
        (2,FALSE,FALSE,4,NULL);


----------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_disk_page (nro_page INTEGER)
  RETURNS INTEGER AS
$BODY$
DECLARE
  contador INTEGER := 0 ;
  resultado REAL := _numero;

BEGIN

	RAISE NOTICE 'Se invoca get_disk_page(%)', _nro_page;

  LOOP
  EXIT WHEN contador = _cantidad ;
    contador := contador + 1 ;
    resultado := resultado + (resultado * _porcentaje / 100);
  END LOOP ;

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