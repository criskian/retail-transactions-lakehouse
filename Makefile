# Runner para Linux y macOS. En Windows usa `invoke` (ver tasks.py): make no
# suele estar instalado y las rutas del venv son distintas.
ifeq ($(OS),Windows_NT)
    PYTHON    := .venv/Scripts/python
else
    PYTHON    := .venv/bin/python
endif

.PHONY: help install pipeline bronze silver gold models export quality \
        ingest ingest-check app test lint fmt doctor docker clean

help:
	@echo "Lakehouse de transacciones — objetivos disponibles"
	@echo ""
	@echo "  make install       crea .venv e instala el pipeline completo"
	@echo "  make doctor        verifica Java, Hadoop y los workers de Python"
	@echo "  make pipeline      bronze -> silver -> gold -> models"
	@echo "  make export        construye data/serving/serving.duckdb"
	@echo "  make quality       valida los contratos de la capa Gold"
	@echo "  make ingest-check  reporta qué cambió en data/landing/"
	@echo "  make ingest        reprocesa si hay datos nuevos"
	@echo "  make app           levanta el dashboard en :8501"
	@echo "  make test / lint   suite de pruebas / ruff"
	@echo "  make clean         borra capas derivadas y cachés"

install:
	python3 -m venv .venv
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements-dev.txt

doctor:
	$(PYTHON) -m src.pipeline.run --doctor

pipeline:
	$(PYTHON) -m src.pipeline.run --step all

bronze silver gold models:
	$(PYTHON) -m src.pipeline.run --step $@

export:
	$(PYTHON) -m src.pipeline.export_serving

quality:
	$(PYTHON) -m src.pipeline.quality

ingest-check:
	$(PYTHON) -m src.pipeline.ingest --check

ingest:
	$(PYTHON) -m src.pipeline.ingest --run

app:
	$(PYTHON) -m streamlit run app/streamlit_app.py

test:
	$(PYTHON) -m pytest -q

lint:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .

fmt:
	$(PYTHON) -m ruff check --fix .
	$(PYTHON) -m ruff format .

docker:
	docker build -t retail-lakehouse .

clean:
	rm -rf data/bronze data/silver data/gold data/models data/serving
	rm -rf .pytest_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
