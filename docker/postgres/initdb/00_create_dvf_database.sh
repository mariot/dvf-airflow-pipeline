#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# PostgreSQL first-boot bootstrap. The official image runs this exactly once,
# when ./pgdata is empty. Delete ./pgdata to run it again.
#
# The cluster ends up with two isolated databases:
#   - airflow  (POSTGRES_DB): Airflow metadata, owned by POSTGRES_USER
#   - dvf      (DVF_DB_NAME): business data, owned by a dedicated least-privilege role
#
# Then it applies /sql/init.sql (the same file the DAG's schema-sync task runs)
# as the dvf role, so the schema exists before the first DAG run and every
# object is owned by that role.
# -----------------------------------------------------------------------------
set -euo pipefail

: "${DVF_DB_NAME:?DVF_DB_NAME must be set}"
: "${DVF_DB_USER:?DVF_DB_USER must be set}"
: "${DVF_DB_PASSWORD:?DVF_DB_PASSWORD must be set}"

echo "[initdb] creating role '${DVF_DB_USER}' and database '${DVF_DB_NAME}'"

psql -v ON_ERROR_STOP=1 \
     --username "${POSTGRES_USER}" \
     --dbname "${POSTGRES_DB}" \
     --set=dvf_db="${DVF_DB_NAME}" \
     --set=dvf_user="${DVF_DB_USER}" \
     --set=dvf_password="${DVF_DB_PASSWORD}" <<-'EOSQL'
	CREATE ROLE :"dvf_user" WITH LOGIN PASSWORD :'dvf_password';
	CREATE DATABASE :"dvf_db" OWNER :"dvf_user" ENCODING 'UTF8' TEMPLATE template0;
	REVOKE ALL ON DATABASE :"dvf_db" FROM PUBLIC;
	GRANT CONNECT, TEMPORARY ON DATABASE :"dvf_db" TO :"dvf_user";
EOSQL

# PostGIS is not a trusted extension, so the superuser creates it; the dvf role
# then owns every object built on top of it (sql/semantic.sql).
echo "[initdb] enabling PostGIS in '${DVF_DB_NAME}'"
psql -v ON_ERROR_STOP=1 \
     --username "${POSTGRES_USER}" \
     --dbname "${DVF_DB_NAME}" \
     --command "CREATE EXTENSION IF NOT EXISTS postgis" \
     --command "CREATE EXTENSION IF NOT EXISTS pgrouting"

echo "[initdb] applying /sql/init.sql to '${DVF_DB_NAME}' as '${DVF_DB_USER}'"

# During initdb the server only listens on the Unix socket, where the image
# trusts local connections, so no password is needed here.
psql -v ON_ERROR_STOP=1 \
     --username "${DVF_DB_USER}" \
     --dbname "${DVF_DB_NAME}" \
     --file /sql/init.sql

echo "[initdb] done"
