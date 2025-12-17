import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import unicodedata
    return mo, pd, unicodedata


@app.cell
def _(pd, unicodedata):
    def normalize_name(s):
        """Normalize name for matching: remove accents, uppercase, strip."""
        if pd.isna(s):
            return ""
        s = unicodedata.normalize("NFD", str(s))
        s = "".join(c for c in s if unicodedata.category(c) != "Mn")
        return s.upper().strip()

    # Load data
    df = pd.read_parquet("data/clasificaciones_2025.parquet")

    # Filter individual runners
    df_individual = df[df["categoria"].isin(["absoluta", "autonomos"])].copy()
    df_individual = df_individual.drop_duplicates(
        subset=["nombre", "empresa", "distancia", "tiempo"], keep="first"
    )

    # Calculate pace
    df_individual["distancia_km"] = df_individual["distancia"].map({"5K": 5, "10K": 10})
    df_individual["ritmo_min_km"] = (
        df_individual["tiempo_segundos"] / 60 / df_individual["distancia_km"]
    )
    df_individual["tiempo_min"] = df_individual["tiempo_segundos"] / 60

    # Distance corrections
    df_individual["distancia_new"] = df_individual["distancia"]
    mask_rapidos = (df_individual["distancia"] == "10K") & (
        df_individual["tiempo_min"] < 40
    )
    df_individual.loc[mask_rapidos, "distancia_new"] = "5K"
    mask_lentos = (df_individual["distancia"] == "5K") & (
        df_individual["tiempo_min"] > 60
    )
    df_individual.loc[mask_lentos, "distancia_new"] = "10K"
    df_individual["distancia_km_new"] = df_individual["distancia_new"].map(
        {"5K": 5, "10K": 10}
    )
    df_individual["ritmo_new"] = (
        df_individual["tiempo_min"] / df_individual["distancia_km_new"]
    )

    # Filter equipos
    df_equipos = df[df["categoria"].str.startswith("equipos")].copy()

    # Associate runners with teams
    df_individual["nombre_norm"] = df_individual["nombre"].apply(normalize_name)
    df_equipos["nombre_norm"] = df_equipos["nombre"].apply(normalize_name)

    equipo_info = df_equipos[
        ["nombre_norm", "nombre_equipo", "num_corredores"]
    ].drop_duplicates()
    equipo_info = equipo_info.drop_duplicates(subset="nombre_norm", keep="first")
    equipo_info = equipo_info.rename(
        columns={"nombre_equipo": "equipo", "num_corredores": "equipo_tipo"}
    )
    df_individual = df_individual.merge(equipo_info, on="nombre_norm", how="left")
    df_individual["equipo"] = df_individual["equipo"].fillna("")
    df_individual["equipo_tipo"] = df_individual["equipo_tipo"].fillna(0).astype(int)

    # Calculate team pace
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
        df_equipos["tiempo_acum_seg"]
        / 60
        / df_equipos["num_corredores"]
        / df_equipos["distancia_km"]
    )

    # Team distance corrections
    df_equipos["tiempo_acum_min"] = df_equipos["tiempo_acum_seg"] / 60
    df_equipos["distancia_new"] = df_equipos["distancia"]
    mask_equipos_rapidos = (df_equipos["distancia"] == "10K") & (
        df_equipos["ritmo_equipo"] < 4
    )
    df_equipos.loc[mask_equipos_rapidos, "distancia_new"] = "5K"
    df_equipos["distancia_km_new"] = df_equipos["distancia_new"].map(
        {"5K": 5, "10K": 10}
    )
    df_equipos["ritmo_equipo_new"] = (
        df_equipos["tiempo_acum_min"]
        / df_equipos["num_corredores"]
        / df_equipos["distancia_km_new"]
    )

    # Unique teams
    df_equipos_unique = df_equipos.drop_duplicates(
        subset=["nombre_equipo", "categoria", "distancia", "sexo"]
    )
    return df_equipos_unique, df_individual


@app.cell
def _(mo):
    mo.md("""
    # Carrera de las Empresas 2025
    **Quironprevención - Madrid**
    """)
    return


@app.cell
def _(df_equipos_unique, df_individual, mo):
    # Stats
    total_runners = len(df_individual)
    total_empresas = df_individual["empresa"].nunique()
    total_equipos = df_equipos_unique["nombre_equipo"].nunique()
    ritmo_5k = df_individual[df_individual["distancia_new"] == "5K"]["ritmo_new"].mean()
    ritmo_10k = df_individual[df_individual["distancia_new"] == "10K"][
        "ritmo_new"
    ].mean()

    def fmt_ritmo(r):
        mins = int(r)
        secs = int((r - mins) * 60)
        return f"{mins}:{secs:02d}/km"

    mo.hstack(
        [
            mo.stat(value=f"{total_runners:,}".replace(",", "."), label="Corredores"),
            mo.stat(value=f"{total_empresas:,}".replace(",", "."), label="Empresas"),
            mo.stat(value=f"{total_equipos:,}".replace(",", "."), label="Equipos"),
            mo.stat(value=fmt_ritmo(ritmo_5k), label="Ritmo Medio 5K"),
            mo.stat(value=fmt_ritmo(ritmo_10k), label="Ritmo Medio 10K"),
        ],
        justify="center",
        gap=2,
    )
    return


@app.cell
def _(mo):
    mo.md("""
    ## Corredores Individuales
    """)
    return


@app.cell
def _(df_individual, mo):
    # Select columns to display
    display_cols = [
        "puesto",
        "nombre",
        "empresa",
        "equipo",
        "sexo",
        "distancia",
        "distancia_new",
        "tiempo",
        "ritmo_min_km",
        "ritmo_new",
    ]
    runners_display = df_individual[display_cols].copy()
    runners_display.columns = [
        "Puesto",
        "Nombre",
        "Empresa",
        "Equipo",
        "Sexo",
        "Dist.",
        "Dist. Corr.",
        "Tiempo",
        "Ritmo",
        "Ritmo Corr.",
    ]
    runners_display["Ritmo"] = runners_display["Ritmo"].round(2)
    runners_display["Ritmo Corr."] = runners_display["Ritmo Corr."].round(2)

    mo.ui.table(runners_display, selection=None)
    return


@app.cell
def _(mo):
    mo.md("""
    ## Equipos
    """)
    return


@app.cell
def _(df_equipos_unique, mo):
    # Select columns
    equipo_display_cols = [
        "puesto",
        "nombre_equipo",
        "empresa",
        "sexo",
        "num_corredores",
        "distancia",
        "distancia_new",
        "tiempo_acumulado",
        "ritmo_equipo",
        "ritmo_equipo_new",
    ]
    equipos_display = df_equipos_unique[equipo_display_cols].dropna().copy()
    equipos_display.columns = [
        "Puesto",
        "Equipo",
        "Empresa",
        "Tipo",
        "Corr.",
        "Dist.",
        "Dist. Corr.",
        "Tiempo Acum.",
        "Ritmo",
        "Ritmo Corr.",
    ]
    equipos_display["Ritmo"] = equipos_display["Ritmo"].round(2)
    equipos_display["Ritmo Corr."] = equipos_display["Ritmo Corr."].round(2)

    mo.ui.table(equipos_display, selection=None)
    return


if __name__ == "__main__":
    app.run()
