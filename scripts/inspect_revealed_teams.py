import os
import tarfile

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TAR_PATH = os.path.join(PROJECT_ROOT, "raw_data", "revealed_teams.tar.gz")

MAX_FILES = 20

print("Abriendo tar.gz...\n")

file_count = 0

with tarfile.open(TAR_PATH, "r|gz") as tar:
    for member in tar:
        # Solo archivos reales
        if not member.isfile():
            continue

        # Solo archivos de equipos
        if not member.name.endswith("_team"):
            continue

        f = tar.extractfile(member)
        if f is None:
            continue

        content = f.read().decode("utf-8")

        print("\n==============================")
        print("Archivo:", member.name)
        print("------------------------------")
        print(content) 
        print("==============================\n")

        file_count += 1

        if file_count >= MAX_FILES:
            break

print(f"\nMostrados {file_count} equipos.")
print("Inspección terminada.")