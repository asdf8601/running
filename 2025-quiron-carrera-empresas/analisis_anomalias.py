#!/usr/bin/env python3
"""Análisis de anomalías en datos de la carrera."""

import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_parquet("data/clasificaciones_2025.parquet")
df = df[df["categoria"].isin(["absoluta", "autonomos"])].copy()
df["tiempo_min"] = df["tiempo_segundos"] / 60
df["distancia_km"] = df["distancia"].map({"5K": 5, "10K": 10})
df["ritmo"] = df["tiempo_min"] / df["distancia_km"]

# Histogramas 2x2: tiempo y ritmo para 5K y 10K
fig, axes = plt.subplots(2, 2, figsize=(12, 10))
for i, dist in enumerate(["5K", "10K"]):
    data = df[df["distancia"] == dist]
    axes[0, i].hist(data["tiempo_min"], bins=50, edgecolor="black", alpha=0.7)
    axes[0, i].set_title(f"Tiempo {dist} (n={len(data):,})")
    axes[0, i].set_xlabel("Minutos")
    axes[1, i].hist(data["ritmo"], bins=50, edgecolor="black", alpha=0.7)
    axes[1, i].set_title(f"Ritmo {dist}")
    axes[1, i].set_xlabel("min/km")
plt.tight_layout()
plt.savefig("histogramas_carrera.png", dpi=150)
print("Guardado: histogramas_carrera.png")

# Crear columnas _new con propuesta de etiqueta corregida
# Por ahora solo corregimos los rápidos (10K que probablemente son 5K)
df["distancia_new"] = df["distancia"]

# 10K con tiempo < 40 min -> probablemente son 5K
mask_rapidos_10k = (df["distancia"] == "10K") & (df["tiempo_min"] < 40)
df.loc[mask_rapidos_10k, "distancia_new"] = "5K"

# Recalcular métricas con la distancia corregida
df["distancia_km_new"] = df["distancia_new"].map({"5K": 5, "10K": 10})
df["ritmo_new"] = df["tiempo_min"] / df["distancia_km_new"]

# Anomalías
print("\n=== ANOMALIAS ===\n")

# 10K con tiempo < 40 min (muy rápido para carrera de empresas)
rapidos_10k = df[mask_rapidos_10k]
print(f"10K con tiempo < 40 min (propuesta: 5K): {len(rapidos_10k)}")
if len(rapidos_10k) > 0:
    print(
        rapidos_10k[
            ["nombre", "empresa", "tiempo", "ritmo", "ritmo_new", "distancia_new"]
        ]
        .sort_values("ritmo")
        .head(15)
    )

# 5K con tiempo > 60 min (posibles 10K mal etiquetados)
lentos_5k = df[(df["distancia"] == "5K") & (df["tiempo_min"] > 60)]
print(f"\n5K con tiempo > 60 min (posibles 10K mal etiquetados): {len(lentos_5k)}")
if len(lentos_5k) > 0:
    print(lentos_5k[["nombre", "empresa", "tiempo", "ritmo"]].head(10))

# Tiempos duplicados (más de 10 personas con exactamente el mismo tiempo)
print("\n=== TIEMPOS DUPLICADOS ===")
for dist in ["5K", "10K"]:
    data = df[df["distancia"] == dist]
    dups = data["tiempo"].value_counts()
    dups = dups[dups > 10]
    print(f"\n{dist} - Tiempos con >10 repeticiones: {len(dups)}")
    if len(dups) > 0:
        print(dups.head(10))
