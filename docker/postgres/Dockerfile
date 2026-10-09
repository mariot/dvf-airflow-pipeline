# PostgreSQL 16 + PostGIS 3, built on the official postgres:16 image.
# postgis/postgis has no arm64 build (Apple Silicon), and staying on the same
# Debian base as postgres:16 keeps an existing ./pgdata compatible (same glibc
# collations, same major version).
FROM postgres:16

RUN apt-get update \
 && apt-get install -y --no-install-recommends "postgresql-${PG_MAJOR}-postgis-3" \
 && rm -rf /var/lib/apt/lists/*
