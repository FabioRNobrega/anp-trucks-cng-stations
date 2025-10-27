APP_NAME=anp-cng-api

docker-build:
	docker compose build

docker-run:
	docker compose up

docker-stop:
	docker compose down

docker-logs:
	docker compose logs -f $(APP_NAME)

clean:
	docker compose down -v
	docker rmi $(APP_NAME) || true
