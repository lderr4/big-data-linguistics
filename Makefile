web-app:
	docker compose down
	docker compose up frontend backend postgres --build
down:
	docker compose down

fix-zero-counts:
	docker exec -i postgres psql -U postgres -d postgres < postgres/fix_zero_counts.sql
	
psql:	
	docker exec -it postgres psql -U postgres -d postgres

generate-dump:	
	docker exec -t postgres pg_dump -U postgres postgres > postgres/dump.sql

copy-dump-to-docker:
	cd ./postgres && \
	docker cp dump.sql postgres:/tmp/dump.sql

migrate-dump-to-rds:
	docker exec postgres \
	psql "postgresql://postgres:${RDS_PASSWORD}@database-1.c6z48aiccjir.us-east-1.rds.amazonaws.com:5432/postgres" \
	-f /tmp/dump.sql


