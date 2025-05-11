# !/bin/bash
# upload local postgres data to RDS for production

docker exec -t postgres pg_dump -U postgres postgres > postgres/dump.sql

cd ./postgres && \
docker cp dump.sql postgres:/tmp/dump.sql


docker exec postgres \
psql "postgresql://${RDS_USERNAME}:${RDS_PASSWORD}@${RDS_ENDPOINT}:${RDS_PORT}/${RDS_DB_NAME}" \
-f /tmp/dump.sql