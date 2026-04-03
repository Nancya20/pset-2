-- Inicialización de PostgreSQL al primer arranque.
-- Docker ejecuta automáticamente los archivos .sql en /docker-entrypoint-initdb.d/

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS clean;

COMMENT ON SCHEMA raw   IS 'Capa raw: datos crudos sin transformaciones de negocio';
COMMENT ON SCHEMA clean IS 'Capa clean: modelo dimensional analítico';
