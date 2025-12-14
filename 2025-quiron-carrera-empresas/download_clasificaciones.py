#!/usr/bin/env python3
"""
Download all classifications from Carrera de las Empresas Quirón 2025.
"""

import asyncio
import re
from pathlib import Path

import aiohttp
import backoff
import pandas as pd
from bs4 import BeautifulSoup

BASE_URL = "https://www.carreradelasempresas.com/clasificaciones25"
MAX_CONCURRENT = 5
RESULTS_PER_PAGE = 100


def tiempo_a_segundos(tiempo: str) -> float | None:
    """Convert time string (HH:MM:SS or MM:SS) to seconds."""
    if not tiempo or tiempo == "-":
        return None
    parts = tiempo.strip().split(":")
    try:
        if len(parts) == 3:
            h, m, s = parts
            return int(h) * 3600 + int(m) * 60 + float(s)
        elif len(parts) == 2:
            m, s = parts
            return int(m) * 60 + float(s)
        else:
            return float(parts[0])
    except ValueError:
        return None


def parse_individual(
    html: str, categoria: str, distancia: str, sexo: str
) -> list[dict]:
    """Parse individual classification table (absoluta/autonomos)."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        return []

    rows = []
    for tr in table.find_all("tr")[1:]:  # Skip header
        cells = tr.find_all("td")
        if len(cells) < 4:
            continue

        puesto = cells[0].get_text(strip=True)
        nombre = cells[1].get_text(strip=True)
        empresa = cells[2].get_text(strip=True)
        tiempo = cells[3].get_text(strip=True)

        rows.append(
            {
                "puesto": int(puesto) if puesto.isdigit() else None,
                "nombre": nombre,
                "empresa": empresa,
                "tiempo": tiempo,
                "tiempo_segundos": tiempo_a_segundos(tiempo),
                "categoria": categoria,
                "distancia": distancia,
                "sexo": sexo,
                "num_corredores": 1,
                "nombre_equipo": None,
                "tiempo_acumulado": None,
            }
        )

    return rows


def parse_equipos(
    html: str, categoria: str, distancia: str, sexo: str, num_corredores: int
) -> list[dict]:
    """Parse team classification table."""
    soup = BeautifulSoup(html, "html.parser")

    rows = []
    # Find all tbody with class "equipo"
    tbodies = soup.find_all("tbody", class_="equipo")

    for tbody in tbodies:
        nombre_equipo = tbody.get("data-equipo", "").upper()
        trs = tbody.find_all("tr")
        if not trs:
            continue

        # First row has team position, name, and first runner
        first_row = trs[0]
        cells = first_row.find_all("td")
        if len(cells) < 4:
            continue

        # Position is in first cell (with rowspan)
        puesto_text = cells[0].get_text(strip=True)
        puesto = int(puesto_text) if puesto_text.isdigit() else None

        # Team info is in second cell - extract accumulated time
        team_cell = cells[1]
        team_text = team_cell.get_text(separator="|", strip=True)
        # Format: "TEAM NAME|Tiempo Acumulado: 3958521"
        tiempo_acumulado_ms = None
        if "Tiempo Acumulado:" in team_text:
            match = re.search(r"Tiempo Acumulado:\s*(\d+)", team_text)
            if match:
                tiempo_acumulado_ms = int(match.group(1))

        # Convert ms to HH:MM:SS
        tiempo_acumulado = None
        if tiempo_acumulado_ms:
            total_seconds = tiempo_acumulado_ms / 1000
            h = int(total_seconds // 3600)
            m = int((total_seconds % 3600) // 60)
            s = int(total_seconds % 60)
            tiempo_acumulado = f"{h:02d}:{m:02d}:{s:02d}"

        # First runner is in cells[2] and cells[3]
        nombre = cells[2].get_text(strip=True)
        tiempo = cells[3].get_text(strip=True)

        rows.append(
            {
                "puesto": puesto,
                "nombre": nombre,
                "empresa": nombre_equipo,
                "tiempo": tiempo,
                "tiempo_segundos": tiempo_a_segundos(tiempo),
                "categoria": categoria,
                "distancia": distancia,
                "sexo": sexo,
                "num_corredores": num_corredores,
                "nombre_equipo": nombre_equipo,
                "tiempo_acumulado": tiempo_acumulado,
            }
        )

        # Remaining rows for other team members
        for tr in trs[1:]:
            member_cells = tr.find_all("td")
            if len(member_cells) >= 2:
                nombre = member_cells[0].get_text(strip=True)
                tiempo = member_cells[1].get_text(strip=True)

                rows.append(
                    {
                        "puesto": puesto,
                        "nombre": nombre,
                        "empresa": nombre_equipo,
                        "tiempo": tiempo,
                        "tiempo_segundos": tiempo_a_segundos(tiempo),
                        "categoria": categoria,
                        "distancia": distancia,
                        "sexo": sexo,
                        "num_corredores": num_corredores,
                        "nombre_equipo": nombre_equipo,
                        "tiempo_acumulado": tiempo_acumulado,
                    }
                )

    return rows


def get_max_page(html: str) -> int:
    """Extract max page number from pagination."""
    soup = BeautifulSoup(html, "html.parser")
    # Look for pagination links
    pagination = soup.find_all("a", href=re.compile(r"page=\d+"))
    max_page = 1
    for link in pagination:
        href = link.get("href", "")
        match = re.search(r"page=(\d+)", href)
        if match:
            page = int(match.group(1))
            max_page = max(max_page, page)
    return max_page


@backoff.on_exception(
    backoff.expo,
    (aiohttp.ClientError, asyncio.TimeoutError),
    max_tries=5,
    max_time=60,
)
async def fetch(session: aiohttp.ClientSession, url: str) -> str:
    """Fetch URL content with exponential backoff retry."""
    async with session.get(url) as response:
        return await response.text()


async def download_absoluta(
    session: aiohttp.ClientSession, semaphore: asyncio.Semaphore
) -> list[dict]:
    """Download all absoluta classifications."""
    all_rows = []

    for distancia in ["5", "10"]:
        for sexo in ["M", "F"]:
            categoria = "absoluta"
            sexo_label = "masculino" if sexo == "M" else "femenino"

            # First fetch to get max pages
            url = f"{BASE_URL}/absoluta.php?sexo={sexo}&carrera={distancia}"
            async with semaphore:
                html = await fetch(session, url)
            max_page = get_max_page(html)
            print(f"  Absoluta {distancia}K {sexo_label}: {max_page} páginas")

            # Parse first page
            rows = parse_individual(html, categoria, f"{distancia}K", sexo)
            all_rows.extend(rows)

            # Fetch remaining pages
            tasks = []
            for page in range(2, max_page + 1):
                page_url = f"{url}&page={page}"
                tasks.append(
                    fetch_and_parse_individual(
                        session, semaphore, page_url, categoria, f"{distancia}K", sexo
                    )
                )

            results = await asyncio.gather(*tasks)
            for rows in results:
                all_rows.extend(rows)

    return all_rows


async def fetch_and_parse_individual(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    url: str,
    categoria: str,
    distancia: str,
    sexo: str,
) -> list[dict]:
    """Fetch and parse individual classification page."""
    async with semaphore:
        html = await fetch(session, url)
    return parse_individual(html, categoria, distancia, sexo)


async def download_autonomos(
    session: aiohttp.ClientSession, semaphore: asyncio.Semaphore
) -> list[dict]:
    """Download all autonomos classifications."""
    all_rows = []

    for distancia in ["5", "10"]:
        for sexo in ["M", "F"]:
            categoria = "autonomos"
            sexo_label = "masculino" if sexo == "M" else "femenino"

            url = f"{BASE_URL}/autonomos.php?sexo={sexo}&carrera={distancia}"
            async with semaphore:
                html = await fetch(session, url)

            rows = parse_individual(html, categoria, f"{distancia}K", sexo)
            print(f"  Autónomos {distancia}K {sexo_label}: {len(rows)} corredores")
            all_rows.extend(rows)

    return all_rows


async def download_equipos(
    session: aiohttp.ClientSession, semaphore: asyncio.Semaphore
) -> list[dict]:
    """Download all team classifications."""
    all_rows = []

    # tipoEquipo format: {5k,10k}{2,3,4}{masc,fem,mixto}
    distancias = ["5k", "10k"]
    num_corredores_list = ["2", "3", "4"]
    tipos = [("masc", "M"), ("fem", "F"), ("mixto", "X")]

    for distancia in distancias:
        for num in num_corredores_list:
            for tipo, sexo in tipos:
                categoria = f"equipos_{num}"
                tipo_equipo = f"{distancia}{num}{tipo}"

                carrera_num = "5" if distancia == "5k" else "10"
                url = f"{BASE_URL}/equipos.php?tipoEquipo={tipo_equipo}&carrera={carrera_num}"
                async with semaphore:
                    html = await fetch(session, url)

                rows = parse_equipos(html, categoria, distancia.upper(), sexo, int(num))
                print(
                    f"  Equipos {distancia.upper()} {num} corredores {tipo}: {len(rows)} filas"
                )
                all_rows.extend(rows)

    return all_rows


async def main():
    """Main function to download all classifications."""
    print("Descargando clasificaciones Carrera de las Empresas Quirón 2025...")
    print()

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    async with aiohttp.ClientSession() as session:
        print("Descargando clasificación absoluta...")
        absoluta_rows = await download_absoluta(session, semaphore)
        print(f"  Total absoluta: {len(absoluta_rows)} corredores")
        print()

        print("Descargando clasificación autónomos...")
        autonomos_rows = await download_autonomos(session, semaphore)
        print(f"  Total autónomos: {len(autonomos_rows)} corredores")
        print()

        print("Descargando clasificación equipos...")
        equipos_rows = await download_equipos(session, semaphore)
        print(f"  Total equipos: {len(equipos_rows)} filas")
        print()

    # Combine all data
    all_rows = absoluta_rows + autonomos_rows + equipos_rows
    df = pd.DataFrame(all_rows)

    # Create output directory
    output_dir = Path("data")
    output_dir.mkdir(exist_ok=True)

    # Save to parquet
    output_path = output_dir / "clasificaciones_2025.parquet"
    df.to_parquet(output_path, index=False)

    print(f"Guardado en {output_path}")
    print(f"Total filas: {len(df)}")
    print()
    print("Resumen por categoría:")
    print(df.groupby(["categoria", "distancia", "sexo"]).size().to_string())


if __name__ == "__main__":
    asyncio.run(main())
