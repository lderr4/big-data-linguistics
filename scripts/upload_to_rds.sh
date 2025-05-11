# !/bin/bash
# upload local postgres data to RDS for production

docker exec -t postgres pg_dump -U postgres postgres > postgres/dump.sql

cd ./postgres && \
docker cp dump.sql postgres:/tmp/dump.sql


docker exec postgres \
psql "postgresql://postgres:${RDS_PASSWORD}@database-1.c6z48aiccjir.us-east-1.rds.amazonaws.com:5432/postgres" \
-f /tmp/dump.sql