FROM postgres:latest

# Set environment variables
ENV POSTGRES_USER user
ENV POSTGRES_PASSWORD password
ENV POSTGRES_DB postgres
ENV PGDATA /data/postgres

COPY scripts/init.sql /docker-entrypoint-initdb.d

# Start PostgreSQL in the background, execute init.sql, then dump the database to a file
RUN set -eux; \
    docker-entrypoint.sh postgres & \
    sleep 40;