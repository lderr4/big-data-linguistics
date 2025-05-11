web-app-local:
	docker compose down
	docker compose up frontend backend postgres --build

web-app-production:
	docker-compose up -d --build frontend backend
down:
	docker compose down

fix-zero-counts:
	docker exec -i postgres psql -U postgres -d postgres < postgres/fix_zero_counts.sql
	
psql:	
	docker exec -it postgres psql -U postgres -d postgres


