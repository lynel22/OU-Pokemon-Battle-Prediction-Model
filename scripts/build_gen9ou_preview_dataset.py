import tarfile
import re
import csv
from collections import defaultdict
from tqdm import tqdm
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

TAR_PATH = os.path.join(PROJECT_ROOT, "raw_data", "revealed_teams.tar.gz")
OUTPUT_CSV = os.path.join(PROJECT_ROOT, "data", "gen9ou_preview_dataset.csv")

print("Abriendo tar.gz en streaming...")

paired_battles = defaultdict(dict)

def extract_pokemon_names(text):
    pokemons = []
    for line in text.splitlines():
        line = line.strip()
        if "@" in line:
            name = line.split("@")[0].strip()
            if name != "$missing_name$":
                pokemons.append(name)
    return pokemons


with tarfile.open(TAR_PATH, "r:gz") as tar:

    members = (
        m for m in tar
        if m.isfile()
        and "gen9ou" in m.name
        and m.name.endswith(".gen9ou_team")
    )

    for member in tqdm(members, desc="Procesando archivos gen9ou"):

        filename = member.name.split("/")[-1]

        match = re.match(r"(.+?)_(.+?)_vs_(.+?)_.*_(WIN|LOSS)\.gen9ou_team", filename)
        if not match:
            continue

        battle_id = match.group(1)
        result = match.group(4)

        f = tar.extractfile(member)
        if not f:
            continue

        text = f.read().decode("utf-8", errors="ignore")
        pokemons = extract_pokemon_names(text)

        if len(pokemons) != 6:
            continue

        paired_battles[battle_id][result] = pokemons


print("\nConstruyendo dataset final con duplicación simétrica...")

rows = []
valid_battles = 0

for battle_id, results in paired_battles.items():

    if "WIN" in results and "LOSS" in results:

        winner_team = results["WIN"]
        loser_team = results["LOSS"]

        rows.append(
            [battle_id] +
            winner_team +
            loser_team +
            [1]
        )

        rows.append(
            [battle_id] +
            loser_team +
            winner_team +
            [0]
        )

        valid_battles += 1


header = (
    ["battle_id"] +
    [f"p1_poke{i}" for i in range(1, 7)] +
    [f"p2_poke{i}" for i in range(1, 7)] +
    ["p1_win"]
)

os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)

with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(header)
    writer.writerows(rows)

print("\n==============================")
print(f"Batallas válidas encontradas: {valid_battles}")
print(f"Filas finales (tras duplicar): {len(rows)}")
print(f"Dataset guardado en: {OUTPUT_CSV}")
print("==============================")