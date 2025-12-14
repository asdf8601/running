#!/usr/bin/env python3
"""
Dashboard for Carrera de las Empresas 2025 with pace metrics and filters.
"""

import json
from pathlib import Path

import pandas as pd

# Load data
df = pd.read_parquet("data/clasificaciones_2025.parquet")

# Filter only individual runners (absoluta + autonomos)
df_individual = df[df["categoria"].isin(["absoluta", "autonomos"])].copy()

# Calculate pace (ritmo) in min/km
df_individual["distancia_km"] = df_individual["distancia"].map({"5K": 5, "10K": 10})
df_individual["ritmo_min_km"] = (
    df_individual["tiempo_segundos"] / 60 / df_individual["distancia_km"]
)

# Filter equipos data
df_equipos = df[df["categoria"].str.startswith("equipos")].copy()


# Calculate team pace - use tiempo_acumulado converted to seconds
def tiempo_acumulado_a_segundos(t):
    if not t or t == "-":
        return None
    parts = t.strip().split(":")
    try:
        if len(parts) == 3:
            h, m, s = parts
            return int(h) * 3600 + int(m) * 60 + int(s)
        elif len(parts) == 2:
            m, s = parts
            return int(m) * 60 + int(s)
    except ValueError:
        return None
    return None


df_equipos["tiempo_acum_seg"] = df_equipos["tiempo_acumulado"].apply(
    tiempo_acumulado_a_segundos
)
df_equipos["distancia_km"] = df_equipos["distancia"].map({"5K": 5, "10K": 10})
df_equipos["ritmo_equipo"] = (
    df_equipos["tiempo_acum_seg"] / 60 / df_equipos["distancia_km"]
)

# Get unique teams (one row per team)
df_equipos_unique = df_equipos.drop_duplicates(
    subset=["nombre_equipo", "categoria", "distancia", "sexo"]
)

# Pace by equipos category
equipos_pace = (
    df_equipos_unique.groupby(["categoria", "distancia", "sexo"])
    .agg(
        ritmo_medio=("ritmo_equipo", "mean"),
        ritmo_mediana=("ritmo_equipo", "median"),
        count=("ritmo_equipo", "count"),
    )
    .reset_index()
)
equipos_pace["ritmo_medio"] = equipos_pace["ritmo_medio"].round(2)
equipos_pace["ritmo_mediana"] = equipos_pace["ritmo_mediana"].round(2)
equipos_pace["num_corredores"] = (
    equipos_pace["categoria"].str.extract(r"(\d)").astype(int)
)

# Equipos data for filtering (one row per team with stats)
equipos_data = (
    df_equipos_unique[
        [
            "nombre_equipo",
            "empresa",
            "categoria",
            "distancia",
            "sexo",
            "tiempo_acumulado",
            "tiempo_acum_seg",
            "ritmo_equipo",
            "puesto",
            "num_corredores",
        ]
    ]
    .dropna()
    .copy()
)
equipos_data["ritmo_equipo"] = equipos_data["ritmo_equipo"].round(2)
equipos_data = equipos_data.to_dict("records")

# Prepare data for charts
# 1. Full runner data for filtering
runner_data = (
    df_individual[
        [
            "nombre",
            "empresa",
            "tiempo",
            "tiempo_segundos",
            "distancia",
            "sexo",
            "categoria",
            "ritmo_min_km",
            "puesto",
        ]
    ]
    .dropna()
    .to_dict("records")
)

# 2. Pace by sex
pace_by_sex = (
    df_individual.groupby(["sexo", "distancia"])
    .agg(
        ritmo_medio=("ritmo_min_km", "mean"),
        ritmo_mediana=("ritmo_min_km", "median"),
        count=("ritmo_min_km", "count"),
    )
    .reset_index()
)
pace_by_sex["ritmo_medio"] = pace_by_sex["ritmo_medio"].round(2)
pace_by_sex["ritmo_mediana"] = pace_by_sex["ritmo_mediana"].round(2)

# 3. All empresa stats (for comparison chart)
all_empresa_stats = (
    df_individual.groupby(["empresa", "sexo", "distancia"])
    .agg(
        ritmo_medio=("ritmo_min_km", "mean"),
        ritmo_mediana=("ritmo_min_km", "median"),
        count=("ritmo_min_km", "count"),
        mejor_tiempo=("tiempo_segundos", "min"),
    )
    .reset_index()
)
all_empresa_stats["ritmo_medio"] = all_empresa_stats["ritmo_medio"].round(2)
all_empresa_stats["ritmo_mediana"] = all_empresa_stats["ritmo_mediana"].round(2)

# 4. Pace by empresa, sexo, distancia (top 15 by best pace per group)
empresa_stats = all_empresa_stats.copy()
# Get top 15 per sexo+distancia combination by best pace
empresa_stats_list = []
for (sexo, dist), group in empresa_stats.groupby(["sexo", "distancia"]):
    top15 = group.nsmallest(15, "ritmo_medio")
    empresa_stats_list.append(top15)
empresa_stats = pd.concat(empresa_stats_list, ignore_index=True)
empresa_stats["ritmo_medio"] = empresa_stats["ritmo_medio"].round(2)
empresa_stats["ritmo_mediana"] = empresa_stats["ritmo_mediana"].round(2)

# 4. Pace distribution data
pace_dist = df_individual[["ritmo_min_km", "distancia", "sexo"]].dropna()

# 5. All empresas for autocomplete
all_empresas = sorted(df_individual["empresa"].unique().tolist())

# Vega-Lite specs
pace_by_sex_spec = {
    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
    "title": "Ritmo Medio por Sexo y Distancia",
    "width": 300,
    "height": 200,
    "data": {"values": pace_by_sex.to_dict("records")},
    "mark": {"type": "bar", "cornerRadiusEnd": 4},
    "encoding": {
        "x": {
            "field": "sexo",
            "type": "nominal",
            "title": "Sexo",
            "axis": {"labelAngle": 0},
        },
        "y": {
            "field": "ritmo_medio",
            "type": "quantitative",
            "title": "Ritmo (min/km)",
        },
        "color": {"field": "distancia", "type": "nominal", "title": "Distancia"},
        "xOffset": {"field": "distancia", "type": "nominal"},
        "tooltip": [
            {"field": "sexo", "title": "Sexo"},
            {"field": "distancia", "title": "Distancia"},
            {"field": "ritmo_medio", "title": "Ritmo Medio (min/km)"},
            {"field": "ritmo_mediana", "title": "Ritmo Mediana (min/km)"},
            {"field": "count", "title": "Corredores"},
        ],
    },
}


# Create separate specs for each sexo+distancia combination
def make_empresa_spec(data, sexo_label, distancia_label):
    sexo_name = "Masculino" if sexo_label == "M" else "Femenino"
    return {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "title": f"Ritmo Empresas - {distancia_label} {sexo_name}",
        "width": 350,
        "height": 300,
        "data": {"values": data},
        "mark": {"type": "bar", "cornerRadiusEnd": 4},
        "encoding": {
            "y": {
                "field": "empresa",
                "type": "nominal",
                "sort": {"field": "ritmo_medio", "order": "ascending"},
                "title": None,
            },
            "x": {
                "field": "ritmo_medio",
                "type": "quantitative",
                "title": "Ritmo (min/km)",
            },
            "color": {
                "field": "ritmo_medio",
                "type": "quantitative",
                "scale": {
                    "scheme": "redyellowgreen",
                    "reverse": True,
                    "domain": [4, 9],
                },
                "legend": None,
            },
            "tooltip": [
                {"field": "empresa", "title": "Empresa"},
                {"field": "ritmo_medio", "title": "Ritmo Medio"},
                {"field": "ritmo_mediana", "title": "Ritmo Mediana"},
                {"field": "count", "title": "Corredores"},
            ],
        },
    }


pace_empresa_5k_m = make_empresa_spec(
    empresa_stats[
        (empresa_stats["sexo"] == "M") & (empresa_stats["distancia"] == "5K")
    ].to_dict("records"),
    "M",
    "5K",
)
pace_empresa_5k_f = make_empresa_spec(
    empresa_stats[
        (empresa_stats["sexo"] == "F") & (empresa_stats["distancia"] == "5K")
    ].to_dict("records"),
    "F",
    "5K",
)
pace_empresa_10k_m = make_empresa_spec(
    empresa_stats[
        (empresa_stats["sexo"] == "M") & (empresa_stats["distancia"] == "10K")
    ].to_dict("records"),
    "M",
    "10K",
)
pace_empresa_10k_f = make_empresa_spec(
    empresa_stats[
        (empresa_stats["sexo"] == "F") & (empresa_stats["distancia"] == "10K")
    ].to_dict("records"),
    "F",
    "10K",
)

# Top 15 equipos by best pace per sexo+distancia
equipos_top_stats = (
    df_equipos_unique[["nombre_equipo", "empresa", "distancia", "sexo", "ritmo_equipo"]]
    .dropna()
    .copy()
)
equipos_top_list = []
for (sexo, dist), group in equipos_top_stats.groupby(["sexo", "distancia"]):
    top15 = group.nsmallest(15, "ritmo_equipo")
    equipos_top_list.append(top15)
equipos_top_stats = pd.concat(equipos_top_list, ignore_index=True)
equipos_top_stats["ritmo_equipo"] = equipos_top_stats["ritmo_equipo"].round(2)


def make_equipo_spec(data, sexo_label, distancia_label):
    sexo_name = (
        "Masculino"
        if sexo_label == "M"
        else ("Femenino" if sexo_label == "F" else "Mixto")
    )
    return {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "title": f"Ritmo Equipos - {distancia_label} {sexo_name}",
        "width": 350,
        "height": 300,
        "data": {"values": data},
        "mark": {"type": "bar", "cornerRadiusEnd": 4},
        "encoding": {
            "y": {
                "field": "nombre_equipo",
                "type": "nominal",
                "sort": {"field": "ritmo_equipo", "order": "ascending"},
                "title": None,
            },
            "x": {
                "field": "ritmo_equipo",
                "type": "quantitative",
                "title": "Ritmo (min/km)",
            },
            "color": {
                "field": "ritmo_equipo",
                "type": "quantitative",
                "scale": {
                    "scheme": "redyellowgreen",
                    "reverse": True,
                    "domain": [4, 9],
                },
                "legend": None,
            },
            "tooltip": [
                {"field": "nombre_equipo", "title": "Equipo"},
                {"field": "empresa", "title": "Empresa"},
                {"field": "ritmo_equipo", "title": "Ritmo"},
            ],
        },
    }


pace_equipo_5k_m = make_equipo_spec(
    equipos_top_stats[
        (equipos_top_stats["sexo"] == "M") & (equipos_top_stats["distancia"] == "5K")
    ].to_dict("records"),
    "M",
    "5K",
)
pace_equipo_5k_f = make_equipo_spec(
    equipos_top_stats[
        (equipos_top_stats["sexo"] == "F") & (equipos_top_stats["distancia"] == "5K")
    ].to_dict("records"),
    "F",
    "5K",
)
pace_equipo_5k_x = make_equipo_spec(
    equipos_top_stats[
        (equipos_top_stats["sexo"] == "X") & (equipos_top_stats["distancia"] == "5K")
    ].to_dict("records"),
    "X",
    "5K",
)
pace_equipo_10k_m = make_equipo_spec(
    equipos_top_stats[
        (equipos_top_stats["sexo"] == "M") & (equipos_top_stats["distancia"] == "10K")
    ].to_dict("records"),
    "M",
    "10K",
)
pace_equipo_10k_f = make_equipo_spec(
    equipos_top_stats[
        (equipos_top_stats["sexo"] == "F") & (equipos_top_stats["distancia"] == "10K")
    ].to_dict("records"),
    "F",
    "10K",
)
pace_equipo_10k_x = make_equipo_spec(
    equipos_top_stats[
        (equipos_top_stats["sexo"] == "X") & (equipos_top_stats["distancia"] == "10K")
    ].to_dict("records"),
    "X",
    "10K",
)

pace_histogram_spec = {
    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
    "title": "Distribución de Ritmos",
    "width": 280,
    "height": 200,
    "data": {"values": pace_dist.to_dict("records")},
    "mark": "bar",
    "encoding": {
        "x": {
            "bin": {"maxbins": 30, "extent": [3, 12]},
            "field": "ritmo_min_km",
            "type": "quantitative",
            "title": "Ritmo (min/km)",
        },
        "y": {"aggregate": "count", "type": "quantitative", "title": "Corredores"},
        "color": {"field": "distancia", "type": "nominal", "title": "Distancia"},
        "column": {"field": "sexo", "type": "nominal", "title": "Sexo"},
    },
}

# Equipos pace chart
equipos_pace_spec = {
    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
    "title": "Ritmo Medio por Equipos",
    "width": 400,
    "height": 250,
    "data": {"values": equipos_pace.to_dict("records")},
    "mark": {"type": "bar", "cornerRadiusEnd": 4},
    "encoding": {
        "x": {
            "field": "num_corredores",
            "type": "ordinal",
            "title": "Corredores por Equipo",
            "axis": {"labelAngle": 0},
        },
        "y": {
            "field": "ritmo_medio",
            "type": "quantitative",
            "title": "Ritmo Medio (min/km)",
        },
        "color": {"field": "distancia", "type": "nominal", "title": "Distancia"},
        "xOffset": {"field": "distancia", "type": "nominal"},
        "row": {"field": "sexo", "type": "nominal", "title": "Sexo"},
        "tooltip": [
            {"field": "categoria", "title": "Categoría"},
            {"field": "distancia", "title": "Distancia"},
            {"field": "sexo", "title": "Sexo"},
            {"field": "ritmo_medio", "title": "Ritmo Medio (min/km)"},
            {"field": "ritmo_mediana", "title": "Ritmo Mediana (min/km)"},
            {"field": "count", "title": "Equipos"},
        ],
    },
}

# Generate HTML dashboard
html_template = """<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Carrera de las Empresas 2025 - Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/vega@5"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-lite@5"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-embed@6"></script>
    <style>
        * { box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            max-width: 1600px;
            margin: 0 auto;
            padding: 2rem;
            background: #f5f5f5;
            color: #333;
        }
        h1 { text-align: center; color: #1e3a5f; margin-bottom: 0.25rem; }
        h2 { color: #1e3a5f; border-bottom: 2px solid #e0e0e0; padding-bottom: 0.5rem; margin-top: 2rem; }
        .subtitle { text-align: center; color: #666; margin-bottom: 2rem; }
        
        .stats {
            display: flex;
            justify-content: center;
            gap: 1.5rem;
            margin-bottom: 2rem;
            flex-wrap: wrap;
        }
        .stat-card {
            background: white;
            padding: 1.25rem 2rem;
            border-radius: 12px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            text-align: center;
            min-width: 150px;
        }
        .stat-value { font-size: 2rem; font-weight: bold; color: #2563eb; }
        .stat-label { color: #666; font-size: 0.85rem; margin-top: 0.25rem; }
        
        .filters {
            background: white;
            padding: 1.5rem;
            border-radius: 12px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            margin-bottom: 2rem;
        }
        .filter-row {
            display: flex;
            gap: 1rem;
            flex-wrap: wrap;
            align-items: flex-end;
        }
        .filter-group { display: flex; flex-direction: column; gap: 0.25rem; }
        .filter-group label { font-size: 0.85rem; color: #666; font-weight: 500; }
        .filter-group input, .filter-group select {
            padding: 0.5rem 0.75rem;
            border: 1px solid #ddd;
            border-radius: 6px;
            font-size: 0.95rem;
            min-width: 200px;
        }
        .filter-group input:focus, .filter-group select:focus {
            outline: none;
            border-color: #2563eb;
            box-shadow: 0 0 0 3px rgba(37,99,235,0.1);
        }
        button {
            padding: 0.5rem 1.25rem;
            background: #2563eb;
            color: white;
            border: none;
            border-radius: 6px;
            cursor: pointer;
            font-size: 0.95rem;
        }
        button:hover { background: #1d4ed8; }
        button.secondary { background: #6b7280; }
        button.secondary:hover { background: #4b5563; }
        
        .charts {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 1.5rem;
        }
        .chart-container {
            background: white;
            padding: 1.25rem;
            border-radius: 12px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            overflow-x: auto;
        }
        .chart-full { grid-column: 1 / -1; }
        
        @media (max-width: 900px) {
            .charts { grid-template-columns: 1fr; }
        }
        
        .results-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9rem;
        }
        .results-table th, .results-table td {
            padding: 0.75rem;
            text-align: left;
            border-bottom: 1px solid #e5e5e5;
        }
        .results-table th {
            background: #f8f9fa;
            font-weight: 600;
            color: #1e3a5f;
            position: sticky;
            top: 0;
        }
        .results-table tr:hover { background: #f0f7ff; }
        .results-table .numeric { text-align: right; font-variant-numeric: tabular-nums; }
        
        .table-wrapper {
            max-height: 500px;
            overflow-y: auto;
            border-radius: 8px;
            border: 1px solid #e5e5e5;
        }
        
        .highlight { background: #fef3c7 !important; }
        
        .badge {
            display: inline-block;
            padding: 0.2rem 0.5rem;
            border-radius: 4px;
            font-size: 0.75rem;
            font-weight: 500;
        }
        .badge-5k { background: #dbeafe; color: #1e40af; }
        .badge-10k { background: #fce7f3; color: #9d174d; }
        .badge-m { background: #e0e7ff; color: #3730a3; }
        .badge-f { background: #fce7f3; color: #9d174d; }
        
        #resultCount { color: #666; font-size: 0.9rem; margin-bottom: 0.5rem; }
    </style>
</head>
<body>
    <h1>Carrera de las Empresas 2025</h1>
    <p class="subtitle">Quirónprevención - Madrid</p>
    
    <div class="stats">
        <div class="stat-card">
            <div class="stat-value">TOTAL_RUNNERS</div>
            <div class="stat-label">Corredores</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">TOTAL_EMPRESAS</div>
            <div class="stat-label">Empresas</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">RITMO_MEDIO_5K</div>
            <div class="stat-label">Ritmo Medio 5K</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">RITMO_MEDIO_10K</div>
            <div class="stat-label">Ritmo Medio 10K</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">TOTAL_TEAMS</div>
            <div class="stat-label">Equipos</div>
        </div>
    </div>

    <h2>Métricas de Ritmo</h2>
    <div class="charts">
        <div class="chart-container" id="paceBySex"></div>
        <div class="chart-container" id="paceHistogram"></div>
    </div>
    
    <h2>Comparar Empresa vs Global</h2>
    <div class="filters" style="margin-bottom: 1rem;">
        <div class="filter-row">
            <div class="filter-group">
                <label for="compareEmpresa">Empresa a comparar</label>
                <input type="text" id="compareEmpresa" list="empresaListCompare" placeholder="Nombre de empresa..." value="SEEDTAG ADVERTISING SL">
                <datalist id="empresaListCompare">
                    ALL_EMPRESAS_OPTIONS
                </datalist>
            </div>
            <button onclick="updateComparison()">Comparar</button>
        </div>
    </div>
    <div class="charts">
        <div class="chart-container" id="compareChart5k"></div>
        <div class="chart-container" id="compareChart10k"></div>
    </div>
    
    <h2>Ritmo por Empresa</h2>
    <p style="color: #666; margin-bottom: 1rem;">Top 15 empresas por mejor ritmo</p>
    <div class="charts">
        <div class="chart-container" id="paceEmpresa5kM"></div>
        <div class="chart-container" id="paceEmpresa10kM"></div>
        <div class="chart-container" id="paceEmpresa5kF"></div>
        <div class="chart-container" id="paceEmpresa10kF"></div>
    </div>
    
    <h2>Ritmo por Equipos</h2>
    <p style="color: #666; margin-bottom: 1rem;">Top 15 equipos por mejor ritmo</p>
    <div class="charts">
        <div class="chart-container" id="paceEquipo5kM"></div>
        <div class="chart-container" id="paceEquipo10kM"></div>
        <div class="chart-container" id="paceEquipo5kF"></div>
        <div class="chart-container" id="paceEquipo10kF"></div>
        <div class="chart-container" id="paceEquipo5kX"></div>
        <div class="chart-container" id="paceEquipo10kX"></div>
    </div>

    <h2>Búsqueda de Corredores</h2>
    <div class="filters">
        <div class="filter-row">
            <div class="filter-group">
                <label for="searchRunner">Buscar Corredor</label>
                <input type="text" id="searchRunner" placeholder="Nombre del corredor...">
            </div>
            <div class="filter-group">
                <label for="searchEmpresa">Empresa</label>
                <input type="text" id="searchEmpresa" list="empresaList" placeholder="Nombre de empresa...">
                <datalist id="empresaList">
                    ALL_EMPRESAS_OPTIONS
                </datalist>
            </div>
            <div class="filter-group">
                <label for="filterDistancia">Distancia</label>
                <select id="filterDistancia">
                    <option value="">Todas</option>
                    <option value="5K">5K</option>
                    <option value="10K">10K</option>
                </select>
            </div>
            <div class="filter-group">
                <label for="filterSexo">Sexo</label>
                <select id="filterSexo">
                    <option value="">Todos</option>
                    <option value="M">Masculino</option>
                    <option value="F">Femenino</option>
                </select>
            </div>
            <button onclick="applyFilters()">Buscar</button>
            <button class="secondary" onclick="clearFilters()">Limpiar</button>
        </div>
    </div>
    
    <div class="chart-container">
        <div id="filterStats" class="stats" style="margin-bottom: 1rem; display: none;"></div>
        <div id="runnerHistogram" style="margin-bottom: 1rem; display: flex; justify-content: center;"></div>
        <div id="resultCount"></div>
        <div class="table-wrapper">
            <table class="results-table" id="resultsTable">
                <thead>
                    <tr>
                        <th class="numeric">#</th>
                        <th class="numeric">Puesto General</th>
                        <th>Nombre</th>
                        <th>Empresa</th>
                        <th>Distancia</th>
                        <th>Sexo</th>
                        <th class="numeric">Tiempo</th>
                        <th class="numeric">Ritmo (min/km)</th>
                    </tr>
                </thead>
                <tbody id="resultsBody"></tbody>
            </table>
        </div>
    </div>

    <h2>Búsqueda de Equipos</h2>
    <div class="filters">
        <div class="filter-row">
            <div class="filter-group">
                <label for="searchEquipoNombre">Buscar Equipo/Empresa</label>
                <input type="text" id="searchEquipoNombre" placeholder="Nombre del equipo o empresa...">
            </div>
            <div class="filter-group">
                <label for="filterEquipoDistancia">Distancia</label>
                <select id="filterEquipoDistancia">
                    <option value="">Todas</option>
                    <option value="5K">5K</option>
                    <option value="10K">10K</option>
                </select>
            </div>
            <div class="filter-group">
                <label for="filterEquipoSexo">Tipo</label>
                <select id="filterEquipoSexo">
                    <option value="">Todos</option>
                    <option value="M">Masculino</option>
                    <option value="F">Femenino</option>
                    <option value="X">Mixto</option>
                </select>
            </div>
            <div class="filter-group">
                <label for="filterEquipoNum">Corredores</label>
                <select id="filterEquipoNum">
                    <option value="">Todos</option>
                    <option value="2">2 corredores</option>
                    <option value="3">3 corredores</option>
                    <option value="4">4 corredores</option>
                </select>
            </div>
            <button onclick="applyEquipoFilters()">Buscar</button>
            <button class="secondary" onclick="clearEquipoFilters()">Limpiar</button>
        </div>
    </div>
    
    <div class="chart-container">
        <div id="equipoFilterStats" class="stats" style="margin-bottom: 1rem; display: none;"></div>
        <div id="equipoHistogram" style="margin-bottom: 1rem; display: flex; justify-content: center;"></div>
        <div id="equipoResultCount"></div>
        <div class="table-wrapper">
            <table class="results-table" id="equiposResultsTable">
                <thead>
                    <tr>
                        <th class="numeric">#</th>
                        <th class="numeric">Puesto</th>
                        <th>Nombre Equipo</th>
                        <th>Empresa</th>
                        <th>Distancia</th>
                        <th>Tipo</th>
                        <th class="numeric">Corredores</th>
                        <th class="numeric">Tiempo Acumulado</th>
                        <th class="numeric">Ritmo (min/km)</th>
                    </tr>
                </thead>
                <tbody id="equiposResultsBody"></tbody>
            </table>
        </div>
    </div>

    <script>
        // Data
        const runnerData = RUNNER_DATA;
        const equiposData = EQUIPOS_DATA;
        const allEmpresas = ALL_EMPRESAS;
        
        // Vega specs
        const paceBySexSpec = PACE_BY_SEX_SPEC;
        const paceHistogramSpec = PACE_HISTOGRAM_SPEC;
        const paceEmpresa5kMSpec = PACE_EMPRESA_5K_M_SPEC;
        const paceEmpresa5kFSpec = PACE_EMPRESA_5K_F_SPEC;
        const paceEmpresa10kMSpec = PACE_EMPRESA_10K_M_SPEC;
        const paceEmpresa10kFSpec = PACE_EMPRESA_10K_F_SPEC;
        const paceEquipo5kMSpec = PACE_EQUIPO_5K_M_SPEC;
        const paceEquipo5kFSpec = PACE_EQUIPO_5K_F_SPEC;
        const paceEquipo5kXSpec = PACE_EQUIPO_5K_X_SPEC;
        const paceEquipo10kMSpec = PACE_EQUIPO_10K_M_SPEC;
        const paceEquipo10kFSpec = PACE_EQUIPO_10K_F_SPEC;
        const paceEquipo10kXSpec = PACE_EQUIPO_10K_X_SPEC;
        
        // Global stats and empresa stats for comparison
        const globalStats = GLOBAL_STATS;
        const allEmpresaStats = ALL_EMPRESA_STATS;
        
        // Render charts
        vegaEmbed('#paceBySex', paceBySexSpec, {actions: false});
        vegaEmbed('#paceHistogram', paceHistogramSpec, {actions: false});
        vegaEmbed('#paceEmpresa5kM', paceEmpresa5kMSpec, {actions: false});
        vegaEmbed('#paceEmpresa5kF', paceEmpresa5kFSpec, {actions: false});
        vegaEmbed('#paceEmpresa10kM', paceEmpresa10kMSpec, {actions: false});
        vegaEmbed('#paceEmpresa10kF', paceEmpresa10kFSpec, {actions: false});
        vegaEmbed('#paceEquipo5kM', paceEquipo5kMSpec, {actions: false});
        vegaEmbed('#paceEquipo5kF', paceEquipo5kFSpec, {actions: false});
        vegaEmbed('#paceEquipo5kX', paceEquipo5kXSpec, {actions: false});
        vegaEmbed('#paceEquipo10kM', paceEquipo10kMSpec, {actions: false});
        vegaEmbed('#paceEquipo10kF', paceEquipo10kFSpec, {actions: false});
        vegaEmbed('#paceEquipo10kX', paceEquipo10kXSpec, {actions: false});
        
        // Comparison chart function
        function updateComparison() {
            const empresaName = document.getElementById('compareEmpresa').value.toUpperCase();
            
            // Build comparison data for 5K
            const data5k = [];
            ['M', 'F'].forEach(sexo => {
                const global = globalStats.find(g => g.sexo === sexo && g.distancia === '5K');
                const empresa = allEmpresaStats.find(e => 
                    e.empresa.toUpperCase() === empresaName && e.sexo === sexo && e.distancia === '5K'
                );
                if (global) {
                    data5k.push({
                        grupo: sexo === 'M' ? 'Masculino' : 'Femenino',
                        tipo: 'Global',
                        ritmo: global.ritmo_medio,
                        count: global.count
                    });
                }
                if (empresa) {
                    data5k.push({
                        grupo: sexo === 'M' ? 'Masculino' : 'Femenino',
                        tipo: empresaName,
                        ritmo: empresa.ritmo_medio,
                        count: empresa.count
                    });
                }
            });
            
            // Build comparison data for 10K
            const data10k = [];
            ['M', 'F'].forEach(sexo => {
                const global = globalStats.find(g => g.sexo === sexo && g.distancia === '10K');
                const empresa = allEmpresaStats.find(e => 
                    e.empresa.toUpperCase() === empresaName && e.sexo === sexo && e.distancia === '10K'
                );
                if (global) {
                    data10k.push({
                        grupo: sexo === 'M' ? 'Masculino' : 'Femenino',
                        tipo: 'Global',
                        ritmo: global.ritmo_medio,
                        count: global.count
                    });
                }
                if (empresa) {
                    data10k.push({
                        grupo: sexo === 'M' ? 'Masculino' : 'Femenino',
                        tipo: empresaName,
                        ritmo: empresa.ritmo_medio,
                        count: empresa.count
                    });
                }
            });
            
            const makeCompareSpec = (data, title) => ({
                "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
                "title": title,
                "width": 300,
                "height": 200,
                "data": {"values": data},
                "mark": {"type": "bar", "cornerRadiusEnd": 4},
                "encoding": {
                    "x": {"field": "grupo", "type": "nominal", "title": null, "axis": {"labelAngle": 0}},
                    "y": {"field": "ritmo", "type": "quantitative", "title": "Ritmo (min/km)", "scale": {"zero": false}},
                    "xOffset": {"field": "tipo", "type": "nominal"},
                    "color": {
                        "field": "tipo", 
                        "type": "nominal", 
                        "title": "Comparación",
                        "scale": {"range": ["#94a3b8", "#2563eb"]}
                    },
                    "tooltip": [
                        {"field": "tipo", "title": "Grupo"},
                        {"field": "grupo", "title": "Sexo"},
                        {"field": "ritmo", "title": "Ritmo (min/km)"},
                        {"field": "count", "title": "Corredores"}
                    ]
                }
            });
            
            vegaEmbed('#compareChart5k', makeCompareSpec(data5k, 'Comparación 5K'), {actions: false});
            vegaEmbed('#compareChart10k', makeCompareSpec(data10k, 'Comparación 10K'), {actions: false});
        }
        
        // Initial comparison
        document.addEventListener('DOMContentLoaded', updateComparison);
        
        // Enter key triggers comparison
        document.getElementById('compareEmpresa').addEventListener('keypress', e => {
            if (e.key === 'Enter') updateComparison();
        });
        
        // Filter functions
        function formatRitmo(r) {
            const mins = Math.floor(r);
            const secs = Math.round((r - mins) * 60);
            return mins + ':' + secs.toString().padStart(2, '0');
        }
        
        function renderHistogram(containerId, data, ritmoField, title) {
            if (data.length === 0) {
                document.getElementById(containerId).innerHTML = '';
                return;
            }
            const spec = {
                "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
                "title": title,
                "width": 500,
                "height": 150,
                "data": {"values": data},
                "mark": "bar",
                "encoding": {
                    "x": {
                        "bin": {"maxbins": 25},
                        "field": ritmoField,
                        "type": "quantitative",
                        "title": "Ritmo (min/km)"
                    },
                    "y": {
                        "aggregate": "count",
                        "type": "quantitative",
                        "title": "Cantidad"
                    },
                    "color": {
                        "field": "distancia",
                        "type": "nominal",
                        "title": "Distancia"
                    }
                }
            };
            vegaEmbed('#' + containerId, spec, {actions: false});
        }
        
        function applyFilters() {
            const searchRunner = document.getElementById('searchRunner').value.toLowerCase();
            const searchEmpresa = document.getElementById('searchEmpresa').value.toLowerCase();
            const filterDistancia = document.getElementById('filterDistancia').value;
            const filterSexo = document.getElementById('filterSexo').value;
            
            // Check if any filter is active
            const hasFilters = searchRunner || searchEmpresa || filterDistancia || filterSexo;
            
            let filtered = runnerData.filter(r => {
                if (searchRunner && !r.nombre.toLowerCase().includes(searchRunner)) return false;
                if (searchEmpresa && !r.empresa.toLowerCase().includes(searchEmpresa)) return false;
                if (filterDistancia && r.distancia !== filterDistancia) return false;
                if (filterSexo && r.sexo !== filterSexo) return false;
                return true;
            });
            
            // Calculate stats for filtered data
            const totalCorredores = filtered.length;
            const empresasUnicas = new Set(filtered.map(r => r.empresa)).size;
            const ritmos = filtered.map(r => r.ritmo_min_km);
            const ritmoMedio = ritmos.length > 0 ? ritmos.reduce((a, b) => a + b, 0) / ritmos.length : 0;
            const ritmoMin = ritmos.length > 0 ? Math.min(...ritmos) : 0;
            const ritmoMax = ritmos.length > 0 ? Math.max(...ritmos) : 0;
            
            // Update filter stats
            const statsContainer = document.getElementById('filterStats');
            statsContainer.style.display = 'flex';
            statsContainer.innerHTML = `
                <div class="stat-card">
                    <div class="stat-value">${totalCorredores.toLocaleString('es-ES')}</div>
                    <div class="stat-label">Corredores</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">${empresasUnicas.toLocaleString('es-ES')}</div>
                    <div class="stat-label">Empresas</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">${formatRitmo(ritmoMedio)}/km</div>
                    <div class="stat-label">Ritmo Medio</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">${formatRitmo(ritmoMin)}/km</div>
                    <div class="stat-label">Mejor Ritmo</div>
                </div>
            `;
            
            // Render histogram
            renderHistogram('runnerHistogram', filtered, 'ritmo_min_km', 'Distribución de Ritmos');
            
            // Sort by ritmo (pace)
            filtered.sort((a, b) => a.ritmo_min_km - b.ritmo_min_km);
            
            // Limit results: 20 if no filters, 500 otherwise
            const limit = hasFilters ? 500 : 20;
            const limited = filtered.slice(0, limit);
            
            // Update count
            const countText = hasFilters 
                ? `Mostrando ${limited.length} de ${filtered.length} resultados`
                : `Top 20 corredores (usa los filtros para buscar)`;
            document.getElementById('resultCount').textContent = countText;
            
            // Render table
            const tbody = document.getElementById('resultsBody');
            tbody.innerHTML = limited.map((r, index) => `
                <tr>
                    <td class="numeric">${index + 1}</td>
                    <td class="numeric">${r.puesto}</td>
                    <td>${r.nombre}</td>
                    <td>${r.empresa}</td>
                    <td><span class="badge badge-${r.distancia.toLowerCase()}">${r.distancia}</span></td>
                    <td><span class="badge badge-${r.sexo.toLowerCase()}">${r.sexo === 'M' ? 'Masc' : 'Fem'}</span></td>
                    <td class="numeric">${r.tiempo}</td>
                    <td class="numeric">${r.ritmo_min_km.toFixed(2)}</td>
                </tr>
            `).join('');
        }
        
        function clearFilters() {
            document.getElementById('searchRunner').value = '';
            document.getElementById('searchEmpresa').value = '';
            document.getElementById('filterDistancia').value = '';
            document.getElementById('filterSexo').value = '';
            applyFilters(); // Show top 20 after clearing
        }
        
        // Equipos filter functions
        function applyEquipoFilters() {
            const searchNombre = document.getElementById('searchEquipoNombre').value.toLowerCase();
            const filterDistancia = document.getElementById('filterEquipoDistancia').value;
            const filterSexo = document.getElementById('filterEquipoSexo').value;
            const filterNum = document.getElementById('filterEquipoNum').value;
            
            // Check if any filter is active
            const hasFilters = searchNombre || filterDistancia || filterSexo || filterNum;
            
            let filtered = equiposData.filter(e => {
                if (searchNombre && !e.nombre_equipo.toLowerCase().includes(searchNombre) && !e.empresa.toLowerCase().includes(searchNombre)) return false;
                if (filterDistancia && e.distancia !== filterDistancia) return false;
                if (filterSexo && e.sexo !== filterSexo) return false;
                if (filterNum && e.num_corredores !== parseInt(filterNum)) return false;
                return true;
            });
            
            // Calculate stats for filtered data
            const totalEquipos = filtered.length;
            const empresasUnicas = new Set(filtered.map(e => e.empresa)).size;
            const ritmos = filtered.map(e => e.ritmo_equipo);
            const ritmoMedio = ritmos.length > 0 ? ritmos.reduce((a, b) => a + b, 0) / ritmos.length : 0;
            const ritmoMin = ritmos.length > 0 ? Math.min(...ritmos) : 0;
            
            // Update filter stats
            const statsContainer = document.getElementById('equipoFilterStats');
            statsContainer.style.display = 'flex';
            statsContainer.innerHTML = `
                <div class="stat-card">
                    <div class="stat-value">${totalEquipos.toLocaleString('es-ES')}</div>
                    <div class="stat-label">Equipos</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">${empresasUnicas.toLocaleString('es-ES')}</div>
                    <div class="stat-label">Empresas</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">${formatRitmo(ritmoMedio)}/km</div>
                    <div class="stat-label">Ritmo Medio</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">${formatRitmo(ritmoMin)}/km</div>
                    <div class="stat-label">Mejor Ritmo</div>
                </div>
            `;
            
            // Render histogram
            renderHistogram('equipoHistogram', filtered, 'ritmo_equipo', 'Distribución de Ritmos de Equipos');
            
            // Sort by ritmo (pace)
            filtered.sort((a, b) => a.ritmo_equipo - b.ritmo_equipo);
            
            // Limit results: 20 if no filters, 500 otherwise
            const limit = hasFilters ? 500 : 20;
            const limited = filtered.slice(0, limit);
            
            // Update count
            const countText = hasFilters 
                ? `Mostrando ${limited.length} de ${filtered.length} resultados`
                : `Top 20 equipos (usa los filtros para buscar)`;
            document.getElementById('equipoResultCount').textContent = countText;
            
            // Render table
            const tbody = document.getElementById('equiposResultsBody');
            const sexoLabels = {'M': 'Masc', 'F': 'Fem', 'X': 'Mixto'};
            tbody.innerHTML = limited.map((e, index) => `
                <tr>
                    <td class="numeric">${index + 1}</td>
                    <td class="numeric">${e.puesto}</td>
                    <td>${e.nombre_equipo}</td>
                    <td>${e.empresa}</td>
                    <td><span class="badge badge-${e.distancia.toLowerCase()}">${e.distancia}</span></td>
                    <td><span class="badge badge-${e.sexo.toLowerCase()}">${sexoLabels[e.sexo] || e.sexo}</span></td>
                    <td class="numeric">${e.num_corredores}</td>
                    <td class="numeric">${e.tiempo_acumulado}</td>
                    <td class="numeric">${e.ritmo_equipo.toFixed(2)}</td>
                </tr>
            `).join('');
        }
        
        function clearEquipoFilters() {
            document.getElementById('searchEquipoNombre').value = '';
            document.getElementById('filterEquipoDistancia').value = '';
            document.getElementById('filterEquipoSexo').value = '';
            document.getElementById('filterEquipoNum').value = '';
            applyEquipoFilters();
        }
        
        // Show top 20 on page load
        document.addEventListener('DOMContentLoaded', () => {
            applyFilters();
            applyEquipoFilters();
        });
        
        // Enter key triggers search
        document.querySelectorAll('.filters input').forEach(input => {
            input.addEventListener('keypress', e => {
                if (e.key === 'Enter') applyFilters();
            });
        });
    </script>
</body>
</html>
"""

# Calculate stats
total_individual = len(df_individual)
total_equipos = df[df["categoria"].str.startswith("equipos")]["nombre_equipo"].nunique()
total_empresas = df_individual["empresa"].nunique()

ritmo_5k = df_individual[df_individual["distancia"] == "5K"]["ritmo_min_km"].mean()
ritmo_10k = df_individual[df_individual["distancia"] == "10K"]["ritmo_min_km"].mean()


def format_ritmo(r):
    mins = int(r)
    secs = int((r - mins) * 60)
    return f"{mins}:{secs:02d}"


# Build empresas options
empresas_options = "\n".join(f'<option value="{e}">' for e in all_empresas)

# Replace placeholders
html = html_template.replace("TOTAL_RUNNERS", f"{total_individual:,}".replace(",", "."))
html = html.replace("TOTAL_TEAMS", f"{total_equipos:,}".replace(",", "."))
html = html.replace("TOTAL_EMPRESAS", f"{total_empresas:,}".replace(",", "."))
html = html.replace("RITMO_MEDIO_5K", format_ritmo(ritmo_5k) + "/km")
html = html.replace("RITMO_MEDIO_10K", format_ritmo(ritmo_10k) + "/km")
html = html.replace("RUNNER_DATA", json.dumps(runner_data))
html = html.replace("EQUIPOS_DATA", json.dumps(equipos_data))
html = html.replace("ALL_EMPRESAS", json.dumps(all_empresas))
html = html.replace("ALL_EMPRESAS_OPTIONS", empresas_options)
html = html.replace("PACE_BY_SEX_SPEC", json.dumps(pace_by_sex_spec))
html = html.replace("PACE_HISTOGRAM_SPEC", json.dumps(pace_histogram_spec))
html = html.replace("PACE_EMPRESA_5K_M_SPEC", json.dumps(pace_empresa_5k_m))
html = html.replace("PACE_EMPRESA_5K_F_SPEC", json.dumps(pace_empresa_5k_f))
html = html.replace("PACE_EMPRESA_10K_M_SPEC", json.dumps(pace_empresa_10k_m))
html = html.replace("PACE_EMPRESA_10K_F_SPEC", json.dumps(pace_empresa_10k_f))
html = html.replace("PACE_EQUIPO_5K_M_SPEC", json.dumps(pace_equipo_5k_m))
html = html.replace("PACE_EQUIPO_5K_F_SPEC", json.dumps(pace_equipo_5k_f))
html = html.replace("PACE_EQUIPO_5K_X_SPEC", json.dumps(pace_equipo_5k_x))
html = html.replace("PACE_EQUIPO_10K_M_SPEC", json.dumps(pace_equipo_10k_m))
html = html.replace("PACE_EQUIPO_10K_F_SPEC", json.dumps(pace_equipo_10k_f))
html = html.replace("PACE_EQUIPO_10K_X_SPEC", json.dumps(pace_equipo_10k_x))
html = html.replace("GLOBAL_STATS", json.dumps(pace_by_sex.to_dict("records")))
html = html.replace(
    "ALL_EMPRESA_STATS", json.dumps(all_empresa_stats.to_dict("records"))
)

# Save dashboard
output_path = Path("dashboard.html")
output_path.write_text(html)
print(f"Dashboard guardado en {output_path}")
print(f"Abre en el navegador: file://{output_path.absolute()}")
