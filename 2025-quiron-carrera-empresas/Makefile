.PHONY: app edit dashboard clean

app:  ## Ejecutar marimo en modo app
	uv run marimo run app.py

edit:  ## Editar el notebook marimo
	uv run marimo edit app.py

dashboard:  ## Generar dashboard HTML estático (legacy)
	uv run python dashboard.py

clean:  ## Limpiar archivos generados
	rm -f dashboard.html histogramas_carrera.png

help:  ## Mostrar ayuda
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
