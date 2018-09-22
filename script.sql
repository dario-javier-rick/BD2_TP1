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