import requests
import csv
import io
import random
from bs4 import BeautifulSoup
from flask import Flask, request, jsonify
import os

# --- Configuración ---
CSV_URL = "https://www.dropbox.com/scl/fi/aawvto3fjvat6vwmizasa/NAL60P202501.csv?rlkey=x192ureknjobrrm7obsmobzvl&st=tre4vojr&dl=1"
BASE_PAPA_DETAIL = "https://pbcpao.gov/Property/Details"
DEFAULT_LIMIT = 50

app = Flask(__name__)

def extraer_detalle(detail_soup, direccion_mostrada, parcel_id_csv):
    def get_text_safe(tag):
        return tag.get_text(strip=True) if tag else None

    pid_tag = detail_soup.find("td", string=lambda t: t and "Parcel Number" in t)
    parcel_id_html = get_text_safe(pid_tag.find_next("td")) if pid_tag else None
    if parcel_id_html:
        parcel_id_html = parcel_id_html.replace("-", "")

    owner_name = get_text_safe(detail_soup.select_one("div.accordion-content table td span"))

    year_built_el = detail_soup.find("td", string=lambda t: t and "Year Built" in t)
    year_built = get_text_safe(year_built_el.find_next("td")) if year_built_el else None

    roof_structure_el = detail_soup.find(
        "td", string=lambda t: t and any(x in t for x in ["Roof Structure", "Roof Framing", "Roof Type"])
    )
    roof_structure = get_text_safe(roof_structure_el.find_next("td")) if roof_structure_el else None

    roof_cover_el = detail_soup.find(
        "td", string=lambda t: t and any(x in t for x in ["Roof Cover", "Roof Material", "Roof Covering"])
    )
    roof_cover = get_text_safe(roof_cover_el.find_next("td")) if roof_cover_el else None

    land_area_el = detail_soup.find("td", string=lambda t: t and "Total Square Feet" in t)
    land_area = get_text_safe(land_area_el.find_next("td")) if land_area_el else None

    return {
        "Dirección": direccion_mostrada,
        "Parcel ID": parcel_id_html or parcel_id_csv,
        "Propietario": owner_name,
        "Año Construcción": year_built,
        "Roof Structure": roof_structure,
        "Roof Cover": roof_cover,
        "Superficie (sqft)": land_area
    }

@app.route("/", methods=["GET"])
def home():
    return jsonify({"mensaje": "API activa. Usa POST /scraper con JSON { zip: 'XXXXX' }"})

@app.route("/scraper", methods=["POST"])
def scraper():
    data = request.get_json()
    if not data or "zip" not in data:
        return jsonify({"error": "Debes enviar un JSON con el campo 'zip'"}), 400

    zip_input = data["zip"]
    objetivo_validas = data.get("limit", DEFAULT_LIMIT)
    try:
        objetivo_validas = int(objetivo_validas)
    except:
        objetivo_validas = DEFAULT_LIMIT

    # --- Descargar CSV en streaming desde Dropbox ---
    try:
        r = requests.get(CSV_URL, stream=True)
        r.raise_for_status()
        content = io.StringIO(r.content.decode("utf-8", errors="ignore"))
        reader = csv.DictReader(content)
    except Exception as e:
        return jsonify({"error": f"No se pudo leer el CSV: {str(e)}"}), 500

    # --- Filtrar solo las filas que coinciden con el ZIP ---
    parcel_data = []
    for row in reader:
        if row.get("PHY_ZIPCD") == zip_input:
            pid = str(row.get("PARCEL_ID")).zfill(17)
            addr = row.get("PHY_ADDR1")
            parcel_data.append((pid, addr))

    if not parcel_data:
        return jsonify({"error": f"No se encontraron parcelas con ZIP {zip_input}"}), 404

    # --- Extraer detalles desde la web de PAPA ---
    resultados = []
    contador_total = 0
    random.shuffle(parcel_data)

    with requests.Session() as session:
        for parcel_id, direccion in parcel_data:
            contador_total += 1
            try:
                detalle_url = f"{BASE_PAPA_DETAIL}?parcelId={parcel_id}"
                r = session.get(detalle_url, timeout=15)
                soup = BeautifulSoup(r.text, "html.parser")
                datos = extraer_detalle(soup, direccion, parcel_id)

                if datos.get("Roof Structure") and datos.get("Roof Cover"):
                    resultados.append(datos)
                    if len(resultados) >= objetivo_validas:
                        break
            except Exception:
                continue

    return jsonify({
        "zip": zip_input,
        "total_revisadas": contador_total,
        "total_validas": len(resultados),
        "resultados": resultados
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)


