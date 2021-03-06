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

-- Todos los frames del buffer estan libres al inicio
TRUNCATE TABLE bufferpool;
INSERT INTO bufferpool (nro_frame, free, dirty, nro_disk_page, last_touch)
VALUES	(1,TRUE,FALSE,NULL,NULL),
        (2,TRUE,FALSE,NULL,NULL),
        (3,TRUE,FALSE,NULL,NULL),
        (4,TRUE,FALSE,NULL,NULL);

SELECT * FROM bufferpool;

CREATE TABLE param_parametros
(
  param_codigo VARCHAR(30),
  param_valor1 INT
);

INSERT INTO param_parametros (param_codigo, param_valor1)
VALUES	('ULTIMO_NRO_PAGINA',null),
        ('CANT_SOLIC_SECUENCIALES',0),
        ('CANT_SOLIC_SECUENCIALES_MAX',50);
	
SELECT * FROM param_parametros;


----------------------------------------------------------------------------------------

-- Función dummy que simula la escritura a disco de una pagina

CREATE OR REPLACE FUNCTION write_pag_to_disk(nro_page INTEGER)
	RETURNS VOID AS
$BODY$
DECLARE
BEGIN

	  RAISE NOTICE 'Se escribe en disco página (%)', nro_page;

END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

--select write_pag_to_disk(1);

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
	vSecuencial INTEGER;
	vPorcentajeMax INTEGER;
	vUltimaPagina INTEGER;
	vRecCursorOrdenado RECORD;
	resultado INTEGER;

BEGIN

	vSecuencial  := 0;

   FOR vRecCursorOrdenado IN
		 SELECT nro_disk_page
			 FROM bufferpool
			 ORDER BY last_touch DESC
   LOOP
      --RAISE NOTICE 'vUltimaPagina (%)', vUltimaPagina;
		  --RAISE NOTICE 'vRecCursorOrdenado (%)', vRecCursorOrdenado.nro_disk_page;
			--RAISE NOTICE 'nSecuencial (%)', vSecuencial ;

		 	IF(vUltimaPagina IS NOT NULL) THEN
				--TODO: Con el -1 se chequean secuencias del tipo 1,2,3,4. Chequear tambien secuencias del tipo 4,3,2,1
				IF (vUltimaPagina - 1 = vRecCursorOrdenado.nro_disk_page) THEN
					vSecuencial  := vSecuencial  + 1;
				ELSE
					EXIT;
				END IF;
			END IF;

		 vUltimaPagina := vRecCursorOrdenado.nro_disk_page;

   END LOOP;

	SELECT param_valor1
	INTO vPorcentajeMax
	FROM param_parametros
	WHERE param_codigo = 'CANT_SOLIC_SECUENCIALES_MAX';

	IF(vSecuencial * 100 / (SELECT COUNT(*) FROM bufferpool) >= vPorcentajeMax)
	THEN
		RAISE NOTICE 'Uso LRU';

		SELECT *
		INTO resultado
		FROM pick_frame_LRU();

	ELSE
		RAISE NOTICE 'Uso MRU';

		SELECT *
		INTO resultado
		FROM pick_frame_MRU();

		UPDATE param_parametros
		SET param_valor1 = 0
		WHERE param_codigo = 'CANT_SOLIC_SECUENCIALES';

	END IF;

	UPDATE param_parametros
	SET param_valor1 = resultado
	WHERE param_codigo = 'ULTIMO_NRO_PAGINA';

	RETURN resultado;

END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

--select pick_frame_139();
----------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION get_disk_page (nro_page INTEGER)
  RETURNS INTEGER AS
$BODY$
DECLARE
  vResultado INTEGER;
	vEsDirty BOOLEAN;
  
BEGIN

	RAISE NOTICE 'Se invoca get_disk_page(%)', nro_page;

	-- Chequeo si la página ya existe en el buffer
	IF EXISTS (SELECT FROM bufferpool WHERE nro_disk_page = nro_page)
	THEN
	  SELECT nro_frame
	  INTO vResultado
	  FROM bufferpool
	  WHERE nro_disk_page = nro_page;
	  
	  RAISE NOTICE 'Acceso a bufferpool. Frame %', vResultado;
	ELSE
	
	  -- Si no existe en el buffer, busco un frame libre
	  SELECT nro_frame
	  INTO vResultado
	  FROM bufferpool 
	  WHERE free = TRUE
	  ORDER BY last_touch
	  LIMIT 1;
	  
	  IF(vResultado IS NULL)
	  THEN
	    -- Si no hay frame libre, busco cual desalojar segun algoritmo
			SELECT *
			INTO vResultado
			FROM pick_frame_139(); -- TODO: Cambiar a otros algoritmos, segun se este probando

			SELECT dirty
			INTO vEsDirty
			FROM bufferpool
			WHERE nro_frame = vResultado;

			-- Si el frame esta dirty, hay que hacer un update en disco antes de pisar el bloque
			IF (vEsDirty = TRUE)
			THEN
				SELECT write_pag_to_disk(nro_page);
			END IF;

	    RAISE NOTICE 'Acceso a disco con reemplazo. Frame %', vResultado;
	  ELSE
	    RAISE NOTICE 'Acceso a disco sin reemplazo. Frame %', vResultado;
	  END IF;

		-- Actualizo buffer
	  UPDATE bufferpool
	  SET nro_disk_page = nro_page,
	    free = FALSE,
	    last_touch = current_timestamp
	  WHERE nro_frame = vResultado;
	  
	END IF;

	RETURN vResultado;

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

-- Traza más eficiente con pick_frame_MRU()

select get_disk_page (101);
select get_disk_page (102);
select get_disk_page (103);
select get_disk_page (105);
select get_disk_page (106);
select get_disk_page (101);

--LRU: 6 llamadas a disco
--MRU: 5 llamadas a disco
--139: 6 llamadas a disco

select * from bufferpool order by nro_frame asc;

-- Traza más eficiente con pick_frame_LRU()

select get_disk_page (101);
select get_disk_page (102);
select get_disk_page (103);
select get_disk_page (104);
select get_disk_page (105);
select get_disk_page (105);
select get_disk_page (104);

--LRU: 5 llamadas a disco
--MRU: 6 llamadas a disco
--139 5  llamadas a disco

select * from bufferpool order by nro_frame asc;

-- Traza más eficiente con pick_frame_139()

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

--LRU: 6 llamadas a disco
--MRU: 7 llamadas a disco
--139: 6 llamadas a disco

select * from bufferpool order by nro_frame asc;

