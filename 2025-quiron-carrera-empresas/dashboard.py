#!/usr/bin/env python3
"""
Dashboard for Carrera de las Empresas 2025 with pace metrics and filters.
"""

import json
import unicodedata
from pathlib import Path

import pandas as pd


def normalize_name(s):
    """Normalize name for matching: remove accents, uppercase, strip."""
    if pd.isna(s):
        return ""
    s = unicodedata.normalize("NFD", str(s))
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return s.upper().strip()


# Load data
df = pd.read_parquet("data/clasificaciones_2025.parquet")

# Filter only individual runners (absoluta + autonomos)
df_individual = df[df["categoria"].isin(["absoluta", "autonomos"])].copy()

# Remove duplicates (same runner appearing multiple times)
df_individual = df_individual.drop_duplicates(
    subset=["nombre", "empresa", "distancia", "tiempo"], keep="first"
)

# Calculate pace (ritmo) in min/km
df_individual["distancia_km"] = df_individual["distancia"].map({"5K": 5, "10K": 10})
df_individual["ritmo_min_km"] = (
    df_individual["tiempo_segundos"] / 60 / df_individual["distancia_km"]
)

# Crear columnas _new con propuesta de etiqueta corregida
df_individual["tiempo_min"] = df_individual["tiempo_segundos"] / 60
df_individual["distancia_new"] = df_individual["distancia"]

# Corrección 1: 10K con tiempo < 40 min (ritmo < 4 min/km) -> probablemente son 5K
mask_rapidos_10k = (df_individual["distancia"] == "10K") & (
    df_individual["tiempo_min"] < 40
)
df_individual.loc[mask_rapidos_10k, "distancia_new"] = "5K"

# Corrección 2: 5K con tiempo > 60 min (ritmo > 12 min/km) -> probablemente son 10K
mask_lentos_5k = (df_individual["distancia"] == "5K") & (
    df_individual["tiempo_min"] > 60
)
df_individual.loc[mask_lentos_5k, "distancia_new"] = "10K"
df_individual["distancia_km_new"] = df_individual["distancia_new"].map(
    {"5K": 5, "10K": 10}
)
df_individual["ritmo_new"] = (
    df_individual["tiempo_min"] / df_individual["distancia_km_new"]
)

# Filter equipos data
df_equipos = df[df["categoria"].str.startswith("equipos")].copy()

# Associate individual runners with their teams
df_individual["nombre_norm"] = df_individual["nombre"].apply(normalize_name)
df_equipos["nombre_norm"] = df_equipos["nombre"].apply(normalize_name)

# Get unique runner-team associations (one per runner) including num_corredores
equipo_info = df_equipos[
    ["nombre_norm", "nombre_equipo", "num_corredores"]
].drop_duplicates()
equipo_info = equipo_info.drop_duplicates(subset="nombre_norm", keep="first")
equipo_info = equipo_info.rename(
    columns={"nombre_equipo": "equipo", "num_corredores": "equipo_tipo"}
)

# Merge to get team for each individual runner
df_individual = df_individual.merge(equipo_info, on="nombre_norm", how="left")
df_individual["equipo"] = df_individual["equipo"].fillna("")
df_individual["equipo_tipo"] = df_individual["equipo_tipo"].fillna(0).astype(int)


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
# Ritmo por corredor = tiempo_acumulado / num_corredores / distancia
# Esto hace el ritmo comparable con los ritmos individuales
df_equipos["ritmo_equipo"] = (
    df_equipos["tiempo_acum_seg"]
    / 60
    / df_equipos["num_corredores"]
    / df_equipos["distancia_km"]
)

# Crear columnas _new para equipos
df_equipos["tiempo_acum_min"] = df_equipos["tiempo_acum_seg"] / 60
df_equipos["distancia_new"] = df_equipos["distancia"]

# Corrección: 10K con ritmo/corredor < 4 min/km -> probablemente son 5K
mask_rapidos_equipos = (df_equipos["distancia"] == "10K") & (
    df_equipos["ritmo_equipo"] < 4
)
df_equipos.loc[mask_rapidos_equipos, "distancia_new"] = "5K"

# (No hay 5K lentos cuando se calcula ritmo por corredor correctamente)
df_equipos["distancia_km_new"] = df_equipos["distancia_new"].map({"5K": 5, "10K": 10})
df_equipos["ritmo_equipo_new"] = (
    df_equipos["tiempo_acum_min"]
    / df_equipos["num_corredores"]
    / df_equipos["distancia_km_new"]
)

# Get unique teams (one row per team)
df_equipos_unique = df_equipos.drop_duplicates(
    subset=["nombre_equipo", "categoria", "distancia", "sexo"]
)

# Equipos data for filtering (one row per team with stats)
equipos_data = (
    df_equipos_unique[
        [
            "puesto",
            "nombre_equipo",
            "empresa",
            "categoria",
            "distancia",
            "distancia_new",
            "distancia_km",
            "distancia_km_new",
            "sexo",
            "num_corredores",
            "tiempo_acumulado",
            "tiempo_acum_seg",
            "tiempo_acum_min",
            "ritmo_equipo",
            "ritmo_equipo_new",
        ]
    ]
    .dropna()
    .copy()
)
equipos_data["ritmo_equipo"] = equipos_data["ritmo_equipo"].round(2)
equipos_data["ritmo_equipo_new"] = equipos_data["ritmo_equipo_new"].round(2)
equipos_data = equipos_data.to_dict("records")

# Prepare data for charts
# 1. Full runner data for filtering
runner_data = (
    df_individual[
        [
            "puesto",
            "nombre",
            "empresa",
            "equipo",
            "equipo_tipo",
            "tiempo",
            "tiempo_segundos",
            "tiempo_min",
            "categoria",
            "distancia",
            "distancia_new",
            "distancia_km",
            "distancia_km_new",
            "sexo",
            "ritmo_min_km",
            "ritmo_new",
        ]
    ]
    .dropna(subset=["ritmo_min_km"])
    .to_dict("records")
)

# 2. All empresas for autocomplete
all_empresas = sorted(df_individual["empresa"].unique().tolist())

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
                <label for="searchEquipo">Equipo</label>
                <input type="text" id="searchEquipo" placeholder="Nombre del equipo...">
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
                <label for="filterDistanciaNew">Dist. Corregida</label>
                <select id="filterDistanciaNew">
                    <option value="">Todas</option>
                    <option value="5K">5K</option>
                    <option value="10K">10K</option>
                    <option value="changed">Solo cambiadas</option>
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
            <div class="filter-group">
                <label for="filterHasEquipo">En Equipo</label>
                <select id="filterHasEquipo">
                    <option value="">Todos</option>
                    <option value="yes">Con equipo</option>
                    <option value="no">Sin equipo</option>
                </select>
            </div>
            <div class="filter-group">
                <label for="filterEquipoTipo">Tipo Equipo</label>
                <select id="filterEquipoTipo">
                    <option value="">Todos</option>
                    <option value="2">2 corredores</option>
                    <option value="3">3 corredores</option>
                    <option value="4">4 corredores</option>
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
                        <th class="numeric">Puesto</th>
                        <th>Nombre</th>
                        <th>Empresa</th>
                        <th>Equipo</th>
                        <th class="numeric">Tipo</th>
                        <th>Categoría</th>
                        <th>Sexo</th>
                        <th>Distancia</th>
                        <th>Dist. Corregida</th>
                        <th class="numeric">Tiempo</th>
                        <th class="numeric">Tiempo (min)</th>
                        <th class="numeric">Ritmo (min/km)</th>
                        <th class="numeric">Ritmo Corregido</th>
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
                <label for="filterEquipoDistanciaNew">Dist. Corregida</label>
                <select id="filterEquipoDistanciaNew">
                    <option value="">Todas</option>
                    <option value="5K">5K</option>
                    <option value="10K">10K</option>
                    <option value="changed">Solo cambiadas</option>
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
                        <th>Categoría</th>
                        <th>Tipo</th>
                        <th class="numeric">Corredores</th>
                        <th>Distancia</th>
                        <th>Dist. Corregida</th>
                        <th class="numeric">Tiempo Acumulado</th>
                        <th class="numeric">Tiempo (min)</th>
                        <th class="numeric">Ritmo (min/km)</th>
                        <th class="numeric">Ritmo Corregido</th>
                    </tr>
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
            const searchEquipo = document.getElementById('searchEquipo').value.toLowerCase();
            const filterDistancia = document.getElementById('filterDistancia').value;
            const filterDistanciaNew = document.getElementById('filterDistanciaNew').value;
            const filterSexo = document.getElementById('filterSexo').value;
            const filterHasEquipo = document.getElementById('filterHasEquipo').value;
            const filterEquipoTipo = document.getElementById('filterEquipoTipo').value;
            
            // Check if any filter is active
            const hasFilters = searchRunner || searchEmpresa || searchEquipo || filterDistancia || filterDistanciaNew || filterSexo || filterHasEquipo || filterEquipoTipo;
            
            let filtered = runnerData.filter(r => {
                if (searchRunner && !r.nombre.toLowerCase().includes(searchRunner)) return false;
                if (searchEmpresa && !r.empresa.toLowerCase().includes(searchEmpresa)) return false;
                if (searchEquipo && (!r.equipo || !r.equipo.toLowerCase().includes(searchEquipo))) return false;
                if (filterDistancia && r.distancia !== filterDistancia) return false;
                if (filterDistanciaNew === 'changed' && r.distancia === r.distancia_new) return false;
                if (filterDistanciaNew && filterDistanciaNew !== 'changed' && r.distancia_new !== filterDistanciaNew) return false;
                if (filterSexo && r.sexo !== filterSexo) return false;
                if (filterHasEquipo === 'yes' && !r.equipo) return false;
                if (filterHasEquipo === 'no' && r.equipo) return false;
                if (filterEquipoTipo && r.equipo_tipo !== parseInt(filterEquipoTipo)) return false;
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
            
            // Limit results: 20 if no filters, 2000 otherwise
            const limit = hasFilters ? 2000 : 20;
            const limited = filtered.slice(0, limit);
            
            // Update count
            const countText = hasFilters 
                ? `Mostrando ${limited.length} de ${filtered.length} resultados`
                : `Top 20 corredores (usa los filtros para buscar)`;
            document.getElementById('resultCount').textContent = countText;
            
            // Render table
            const tbody = document.getElementById('resultsBody');
            tbody.innerHTML = limited.map((r, index) => {
                const changed = r.distancia !== r.distancia_new;
                const highlightClass = changed ? ' class="highlight"' : '';
                return `
                <tr${highlightClass}>
                    <td class="numeric">${index + 1}</td>
                    <td class="numeric">${r.puesto}</td>
                    <td>${r.nombre}</td>
                    <td>${r.empresa}</td>
                    <td>${r.equipo || '-'}</td>
                    <td class="numeric">${r.equipo_tipo || '-'}</td>
                    <td>${r.categoria}</td>
                    <td><span class="badge badge-${r.sexo.toLowerCase()}">${r.sexo === 'M' ? 'Masc' : 'Fem'}</span></td>
                    <td><span class="badge badge-${r.distancia.toLowerCase()}">${r.distancia}</span></td>
                    <td><span class="badge badge-${r.distancia_new.toLowerCase()}">${r.distancia_new}</span></td>
                    <td class="numeric">${r.tiempo}</td>
                    <td class="numeric">${r.tiempo_min.toFixed(2)}</td>
                    <td class="numeric">${r.ritmo_min_km.toFixed(2)}</td>
                    <td class="numeric">${r.ritmo_new.toFixed(2)}</td>
                </tr>
            `}).join('');
        }
        
        function clearFilters() {
            document.getElementById('searchRunner').value = '';
            document.getElementById('searchEmpresa').value = '';
            document.getElementById('searchEquipo').value = '';
            document.getElementById('filterDistancia').value = '';
            document.getElementById('filterDistanciaNew').value = '';
            document.getElementById('filterSexo').value = '';
            document.getElementById('filterHasEquipo').value = '';
            document.getElementById('filterEquipoTipo').value = '';
            applyFilters(); // Show top 20 after clearing
        }
        
        // Equipos filter functions
        function applyEquipoFilters() {
            const searchNombre = document.getElementById('searchEquipoNombre').value.toLowerCase();
            const filterDistancia = document.getElementById('filterEquipoDistancia').value;
            const filterDistanciaNew = document.getElementById('filterEquipoDistanciaNew').value;
            const filterSexo = document.getElementById('filterEquipoSexo').value;
            const filterNum = document.getElementById('filterEquipoNum').value;
            
            // Check if any filter is active
            const hasFilters = searchNombre || filterDistancia || filterDistanciaNew || filterSexo || filterNum;
            
            let filtered = equiposData.filter(e => {
                if (searchNombre && !e.nombre_equipo.toLowerCase().includes(searchNombre) && !e.empresa.toLowerCase().includes(searchNombre)) return false;
                if (filterDistancia && e.distancia !== filterDistancia) return false;
                if (filterDistanciaNew === 'changed' && e.distancia === e.distancia_new) return false;
                if (filterDistanciaNew && filterDistanciaNew !== 'changed' && e.distancia_new !== filterDistanciaNew) return false;
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
            
            // Limit results: 20 if no filters, 2000 otherwise
            const limit = hasFilters ? 2000 : 20;
            const limited = filtered.slice(0, limit);
            
            // Update count
            const countText = hasFilters 
                ? `Mostrando ${limited.length} de ${filtered.length} resultados`
                : `Top 20 equipos (usa los filtros para buscar)`;
            document.getElementById('equipoResultCount').textContent = countText;
            
            // Render table
            const tbody = document.getElementById('equiposResultsBody');
            const sexoLabels = {'M': 'Masc', 'F': 'Fem', 'X': 'Mixto'};
            tbody.innerHTML = limited.map((e, index) => {
                const changed = e.distancia !== e.distancia_new;
                const highlightClass = changed ? ' class="highlight"' : '';
                return `
                <tr${highlightClass}>
                    <td class="numeric">${index + 1}</td>
                    <td class="numeric">${e.puesto}</td>
                    <td>${e.nombre_equipo}</td>
                    <td>${e.empresa}</td>
                    <td>${e.categoria}</td>
                    <td><span class="badge badge-${e.sexo.toLowerCase()}">${sexoLabels[e.sexo] || e.sexo}</span></td>
                    <td class="numeric">${e.num_corredores}</td>
                    <td><span class="badge badge-${e.distancia.toLowerCase()}">${e.distancia}</span></td>
                    <td><span class="badge badge-${e.distancia_new.toLowerCase()}">${e.distancia_new}</span></td>
                    <td class="numeric">${e.tiempo_acumulado}</td>
                    <td class="numeric">${e.tiempo_acum_min.toFixed(2)}</td>
                    <td class="numeric">${e.ritmo_equipo.toFixed(2)}</td>
                    <td class="numeric">${e.ritmo_equipo_new.toFixed(2)}</td>
                </tr>
            `}).join('');
        }
        
        function clearEquipoFilters() {
            document.getElementById('searchEquipoNombre').value = '';
            document.getElementById('filterEquipoDistancia').value = '';
            document.getElementById('filterEquipoDistanciaNew').value = '';
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

# Usar ritmo_new y distancia_new para las métricas corregidas
ritmo_5k = df_individual[df_individual["distancia_new"] == "5K"]["ritmo_new"].mean()
ritmo_10k = df_individual[df_individual["distancia_new"] == "10K"]["ritmo_new"].mean()


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

# Save dashboard
output_path = Path("dashboard.html")
output_path.write_text(html)
print(f"Dashboard guardado en {output_path}")
print(f"Abre en el navegador: file://{output_path.absolute()}")
