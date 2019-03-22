# Pelican

Microservice for PostgreSQL data import-export to PFB.

## Running

### `docker-compose`

* `feat/microservice` branch from `compose-services`.

### Local

1. `pipenv install` or `pip install -r requirements.txt`
2. Change configuration inside `config.py` (default should be fine for `localhost` Postgres)
3. `python app.py`
