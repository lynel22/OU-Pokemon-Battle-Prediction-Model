import tarfile
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

TAR_PATH = os.path.join(PROJECT_ROOT, "raw_data", "revealed_teams.tar.gz")
MAX_FILES = 1000

def analyze_team(text):
    missing_moves = text.count("$missing_move$")
    missing_items = text.count("$missing_item$")
    missing_abilities = text.count("$missing_ability$")
    missing_evs = text.count("$missing_ev$")
    missing_natures = text.count("$missing_nature$")
    
    pokemon_count = text.count("@")

    return {
        "pokemon": pokemon_count,
        "missing_moves": missing_moves,
        "missing_items": missing_items,
        "missing_abilities": missing_abilities,
        "missing_evs": missing_evs,
        "missing_natures": missing_natures
    }

print("Abriendo tar.gz...")

stats = []
count = 0

with tarfile.open(TAR_PATH, "r:gz") as tar:
    for member in tar:
        if not member.isfile():
            continue

        if not member.name.endswith(".gen4nu_team"):
            continue

        f = tar.extractfile(member)
        if not f:
            continue

        text = f.read().decode("utf-8", errors="ignore")
        stats.append(analyze_team(text))

        count += 1
        if count >= MAX_FILES:
            break

print(f"\nAnalizados {count} equipos.\n")

avg_missing_moves = sum(s["missing_moves"] for s in stats) / count
avg_pokemon = sum(s["pokemon"] for s in stats) / count

print(f"Promedio Pokémon por equipo: {avg_pokemon:.2f}")
print(f"Promedio moves faltantes: {avg_missing_moves:.2f}")