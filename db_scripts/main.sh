#!/usr/bin/env bash
set -Eeuo pipefail

RDSHOST="${RDSHOST:?Debes exportar RDSHOST}"
RDSPORT="${RDSPORT:-5432}"
MASTER_USER="${MASTER_USER:-bookstore_admin}"
DB_NAME="${DB_NAME:-bookstore}"
QUERIES_DIR="${QUERIES_DIR:-./queries}"

export PGPASSWORD="Passwd123456!*"

echo "==> Validando conexión a RDS"
pg_isready -h "$RDSHOST" -p "$RDSPORT" -U "$MASTER_USER" -d postgres

echo "==> Validando archivos SQL"
for f in \
  "${QUERIES_DIR}/create_role.sql" \
  "${QUERIES_DIR}/create_database.sql" \
  "${QUERIES_DIR}/schema.sql" \
  "${QUERIES_DIR}/seed_data.sql" \
  "${QUERIES_DIR}/grants.sql" \
  "${QUERIES_DIR}/verify.sql"
do
  [[ -f "$f" ]] || { echo "ERROR: No existe $f"; exit 1; }
done

echo "==> Creando rol de aplicación"
psql -h "$RDSHOST" -p "$RDSPORT" -U "$MASTER_USER" -d postgres \
  -v ON_ERROR_STOP=1 -f "${QUERIES_DIR}/create_role.sql"

echo "==> Creando base de datos si no existe"
psql -h "$RDSHOST" -p "$RDSPORT" -U "$MASTER_USER" -d postgres \
  -v ON_ERROR_STOP=1 -f "${QUERIES_DIR}/create_database.sql"

echo "==> Creando esquema, tablas, índices y triggers"
psql -h "$RDSHOST" -p "$RDSPORT" -U "$MASTER_USER" -d "$DB_NAME" \
  -v ON_ERROR_STOP=1 -f "${QUERIES_DIR}/schema.sql"

echo "==> Insertando datos semilla"
psql -h "$RDSHOST" -p "$RDSPORT" -U "$MASTER_USER" -d "$DB_NAME" \
  -v ON_ERROR_STOP=1 -f "${QUERIES_DIR}/seed_data.sql"

echo "==> Asignando permisos"
psql -h "$RDSHOST" -p "$RDSPORT" -U "$MASTER_USER" -d "$DB_NAME" \
  -v ON_ERROR_STOP=1 -f "${QUERIES_DIR}/grants.sql"

echo "==> Ejecutando validaciones"
psql -h "$RDSHOST" -p "$RDSPORT" -U "$MASTER_USER" -d "$DB_NAME" \
  -v ON_ERROR_STOP=1 -f "${QUERIES_DIR}/verify.sql"

echo ""
echo "===================================="
echo "✔ Bootstrap completado correctamente"
echo "Host: $RDSHOST"
echo "Port: $RDSPORT"
echo "DB: $DB_NAME"
echo "Admin user: $MASTER_USER"
echo "Queries dir: $QUERIES_DIR"
echo "===================================="