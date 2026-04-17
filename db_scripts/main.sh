#!/usr/bin/env bash
set -Eeuo pipefail

# =========================================================
# VARIABLES
# =========================================================
RDS_HOST="${RDS_HOST:?Debes exportar RDS_HOST con el endpoint de RDS}"
RDS_PORT="${RDS_PORT:-5432}"
MASTER_USER="${MASTER_USER:-bookstore_admin}"
DB_NAME="${DB_NAME:-bookstore}"

# Password del master user de RDS
export PGPASSWORD="${PGPASSWORD:?Debes exportar PGPASSWORD con la clave del master user}"

SQL_DIR="${SQL_DIR:-./sql}"

# =========================================================
# VALIDACIONES
# =========================================================
command -v psql >/dev/null 2>&1 || {
  echo "ERROR: psql no está instalado en el Bastion Host."
  exit 1
}

for f in \
  "${SQL_DIR}/create_data_base.sql" \
  "${SQL_DIR}/schema.sql" \
  "${SQL_DIR}/seed_data.sql" \
  "${SQL_DIR}/grants.sql"
do
  [[ -f "$f" ]] || {
    echo "ERROR: No existe el archivo $f"
    exit 1
  }
done

echo "==> Probando conectividad al endpoint RDS"
pg_isready -h "${RDS_HOST}" -p "${RDS_PORT}" -U "${MASTER_USER}" -d postgres

echo "==> Creando base de datos si no existe"
psql \
  -h "${RDS_HOST}" \
  -p "${RDS_PORT}" \
  -U "${MASTER_USER}" \
  -d postgres \
  -v ON_ERROR_STOP=1 \
  -f "${SQL_DIR}/00_create_database.sql"

echo "==> Creando esquema, tablas, índices y triggers"
psql \
  -h "${RDS_HOST}" \
  -p "${RDS_PORT}" \
  -U "${MASTER_USER}" \
  -d "${DB_NAME}" \
  -v ON_ERROR_STOP=1 \
  -f "${SQL_DIR}/01_schema.sql"

echo "==> Cargando datos semilla"
psql \
  -h "${RDS_HOST}" \
  -p "${RDS_PORT}" \
  -U "${MASTER_USER}" \
  -d "${DB_NAME}" \
  -v ON_ERROR_STOP=1 \
  -f "${SQL_DIR}/02_seed_data.sql"

echo "==> Creando usuario(s) y asignando permisos"
psql \
  -h "${RDS_HOST}" \
  -p "${RDS_PORT}" \
  -U "${MASTER_USER}" \
  -d "${DB_NAME}" \
  -v ON_ERROR_STOP=1 \
  -f "${SQL_DIR}/03_grants.sql"

if [[ -f "${SQL_DIR}/99_verify.sql" ]]; then
  echo "==> Ejecutando validaciones"
  psql \
    -h "${RDS_HOST}" \
    -p "${RDS_PORT}" \
    -U "${MASTER_USER}" \
    -d "${DB_NAME}" \
    -v ON_ERROR_STOP=1 \
    -f "${SQL_DIR}/99_verify.sql"
fi

echo "==> Bootstrap completado correctamente"
echo "Host: ${RDS_HOST}"
echo "Base: ${DB_NAME}"
echo "Usuario administrador usado: ${MASTER_USER}"