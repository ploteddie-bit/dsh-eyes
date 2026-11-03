#!/usr/bin/env python3
import argparse
import atexit
import base64
import json
import os
from pathlib import Path
import re
import subprocess
import urllib.request

RACINE = Path(__file__).resolve().parents[1]


def executer(commande, env=None):
    return subprocess.run(
        commande,
        cwd=RACINE,
        env=env,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout.strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("cible")
    parser.add_argument("--titre", default="Agent achat M2M")
    args = parser.parse_args()
    token = os.environ.get("DSH_VISION_TOKEN")
    if not token:
        raise SystemExit("DSH_VISION_TOKEN est requis dans l’environnement")

    image_wsl = RACINE / "target" / f"vision-clic-{os.getpid()}.png"
    image_wsl.touch(mode=0o600, exist_ok=False)
    atexit.register(lambda: image_wsl.unlink(missing_ok=True))
    image_windows = executer(["wslpath", "-w", str(image_wsl)])
    sortie_capture = executer([
        str(RACINE / "bin" / "capture-ecran.sh"),
        "--fenetre",
        args.titre,
        "--out",
        image_windows,
    ])
    dimensions = re.search(r"OK (\d+)x(\d+)", sortie_capture)
    if not dimensions:
        raise RuntimeError("dimensions de capture absentes")
    largeur, hauteur = map(int, dimensions.groups())

    fenetre = json.loads(executer([
        str(RACINE / "target" / "os-lire.exe"),
        "--titre",
        args.titre,
    ]))
    image_b64 = base64.b64encode(image_wsl.read_bytes()).decode()
    corps = json.dumps({
        "image_b64": image_b64,
        "cible": args.cible,
        "largeur": largeur,
        "hauteur": hauteur,
    }).encode()
    url = os.environ.get("DSH_VISION_URL", "http://127.0.0.1:8445/v2/vision/locate")
    requete = urllib.request.Request(
        url,
        data=corps,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(requete, timeout=180) as reponse:
        data = json.load(reponse)
    mesure = data["result"]
    x = round(fenetre["fenetre_x"] + mesure["x"] + mesure["largeur"] / 2)
    y = round(fenetre["fenetre_y"] + mesure["y"] + mesure["hauteur"] / 2)
    env = os.environ.copy()
    env["OS_TITRE"] = args.titre
    print(executer([str(RACINE / "bin" / "os-clic.sh"), str(x), str(y)], env=env))
    print(json.dumps({"cible": args.cible, "x": x, "y": y, "tier": data["tier"]}))


if __name__ == "__main__":
    main()
