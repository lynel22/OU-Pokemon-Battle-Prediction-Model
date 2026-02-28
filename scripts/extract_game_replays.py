import os
import lz4.frame
import json
import pandas as pd
from tqdm import tqdm

# -------- RUTAS --------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))

ROOT_DIR = os.path.join(PROJECT_ROOT, "raw_data", "gen4ou")
OUTPUT_PATH = os.path.join(PROJECT_ROOT, "data", "battles_gen4_phase1.csv")

MAX_BATTLES = 100  # Para prueba rápida

data_rows = []
battle_count = 0
skipped_count = 0

print("Escaneando archivos...")

# -------- RECOLECTAR ARCHIVOS --------
all_files = []

for root, dirs, files in os.walk(ROOT_DIR):
    for file in files:
        if file.endswith(".json.lz4"):
            all_files.append(os.path.join(root, file))

print(f"Total archivos encontrados: {len(all_files)}\n")
print("Iniciando procesamiento...\n")

# -------- PROCESAMIENTO --------
for path in tqdm(all_files, desc="Procesando partidas"):

    if battle_count >= MAX_BATTLES:
        break

    file_name = os.path.basename(path)
    winner_label = 1 if "_WIN.json.lz4" in file_name else 0

    try:
        with lz4.frame.open(path, "rb") as f:
            data = json.loads(f.read().decode("utf-8"))
    except Exception as e:
        print(f"[ERROR] No se pudo leer {file_name}: {e}")
        continue

    states = data.get("states", [])
    if not states:
        skipped_count += 1
        continue

    first_state = states[0]

    # -------- VALIDAR P1 Y P2 --------
    if "p1" not in first_state or "p2" not in first_state:
        skipped_count += 1
        continue

    try:
        # Extraer equipos
        p1_team = [p["species"] for p in first_state["p1"].get("team", [])]
        p2_team = [p["species"] for p in first_state["p2"].get("team", [])]

        if not p1_team or not p2_team:
            skipped_count += 1
            continue

        # Rellenar hasta 6 slots
        while len(p1_team) < 6:
            p1_team.append(None)
        while len(p2_team) < 6:
            p2_team.append(None)

        row = {
            "p1_pokemon_1": p1_team[0],
            "p1_pokemon_2": p1_team[1],
            "p1_pokemon_3": p1_team[2],
            "p1_pokemon_4": p1_team[3],
            "p1_pokemon_5": p1_team[4],
            "p1_pokemon_6": p1_team[5],
            "p2_pokemon_1": p2_team[0],
            "p2_pokemon_2": p2_team[1],
            "p2_pokemon_3": p2_team[2],
            "p2_pokemon_4": p2_team[3],
            "p2_pokemon_5": p2_team[4],
            "p2_pokemon_6": p2_team[5],
            "winner": winner_label
        }

        data_rows.append(row)
        battle_count += 1

    except Exception as e:
        print(f"[ERROR] Fallo extrayendo equipo de {file_name}: {e}")
        skipped_count += 1
        continue

print("\nProcesamiento terminado.")
print(f"Partidas procesadas: {battle_count}")
print(f"Partidas saltadas (sin p1/p2): {skipped_count}")

# -------- CREAR DATAFRAME --------
df = pd.DataFrame(data_rows)

print("\nPrimeras filas:")
print(df.head())
print("\nShape:", df.shape)

# Crear carpeta data si no existe
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

df.to_csv(OUTPUT_PATH, index=False)
print(f"\nCSV guardado en {OUTPUT_PATH}")