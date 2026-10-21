#!/usr/bin/env python3
"""rec_ecran_cli.py — enregistreur d'écran CLI pour DSH Eyes.

Capture l'écran Windows via mss et encode en MP4 (imageio-ffmpeg), sans GUI.
Arrêt par durée ou par fichier sentinelle (--stop-file), ce qui permet au
démon de piloter un enregistrement long sans garder de socket ouverte.

Sortie : une ligne JSON sur stdout.
  {"ok": true, "path": "...", "frames": N, "duration_s": X, "fps": F}

Dépendances : mss, opencv-python, imageio, imageio-ffmpeg, numpy.
"""
import argparse
import json
import os
import sys
import time

try:
    import mss
    import cv2
    import numpy as np
    import imageio
except ImportError as exc:  # message JSON, jamais une trace brute
    print(json.dumps({"ok": False, "error": "dépendance manquante: " + str(exc)}))
    sys.exit(1)


RESOLUTIONS = {
    "480p": (854, 480),
    "720p": (1280, 720),
    "1080p": (1920, 1080),
}


def dessiner_curseur(frame, x, y, offset_x, offset_y, scale_x, scale_y):
    """Dessine un petit curseur triangulaire à la position écran réelle."""
    cx = int((x - offset_x) * scale_x)
    cy = int((y - offset_y) * scale_y)
    h, w = frame.shape[:2]
    if not (0 <= cx < w and 0 <= cy < h):
        return
    pts = np.array([[cx, cy], [cx, cy + 14], [cx + 4, cy + 10], [cx + 10, cy + 10]], dtype=np.int32)
    cv2.fillPoly(frame, [pts], (255, 255, 255))
    cv2.polylines(frame, [pts], True, (0, 0, 0), 1, cv2.LINE_AA)


def position_curseur():
    """Position du curseur Windows via ctypes (aucune dépendance)."""
    try:
        import ctypes

        class POINT(ctypes.Structure):
            _fields_ = [("x", ctypes.c_long), ("y", ctypes.c_long)]

        pt = POINT()
        if ctypes.windll.user32.GetCursorPos(ctypes.byref(pt)):
            return pt.x, pt.y
    except Exception:
        pass
    return None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out", required=True, help="fichier MP4 de sortie")
    p.add_argument("--fps", type=int, default=20)
    p.add_argument("--resolution", default="720p", choices=["480p", "720p", "1080p", "native"])
    p.add_argument("--duration", type=float, default=0, help="secondes (0 = illimité)")
    p.add_argument("--stop-file", default="", help="fichier dont l'apparition arrête la capture")
    p.add_argument("--monitor", type=int, default=1, help="index mss (0 = tous les écrans)")
    p.add_argument("--cursor", action="store_true", help="dessiner le curseur")
    args = p.parse_args()

    fps = max(1, min(args.fps, 30))
    out = os.path.abspath(args.out)
    # imageio détecte le backend par l'extension : « .tmp » échoue. On garde
    # un nom temporaire reconnu, renommé atomiquement à la fin.
    tmp = out + ".part.mp4"
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    if os.path.exists(tmp):
        os.remove(tmp)

    sct = mss.MSS()
    idx = args.monitor if 0 <= args.monitor < len(sct.monitors) else 1
    monitor = sct.monitors[idx]
    src_w, src_h = monitor["width"], monitor["height"]

    if args.resolution == "native":
        target_w, target_h = src_w, src_h
    else:
        target_w, target_h = RESOLUTIONS[args.resolution]
    target_w -= target_w % 2
    target_h -= target_h % 2
    scale_x = target_w / src_w
    scale_y = target_h / src_h

    writer = imageio.get_writer(
        tmp,
        fps=fps,
        codec="libx264",
        pixelformat="yuv420p",
        output_params=["-crf", "23", "-preset", "fast"],
        macro_block_size=None,
    )

    frames = 0
    debut = time.time()
    frame_duration = 1.0 / fps
    try:
        while True:
            if args.duration and (time.time() - debut) >= args.duration:
                break
            if args.stop_file and os.path.exists(args.stop_file):
                break
            t0 = time.time()
            img = np.array(sct.grab(monitor))
            frame = cv2.cvtColor(img, cv2.COLOR_BGRA2RGB)
            if (target_w, target_h) != (src_w, src_h):
                frame = cv2.resize(frame, (target_w, target_h), interpolation=cv2.INTER_AREA)
            if args.cursor:
                pos = position_curseur()
                if pos:
                    dessiner_curseur(
                        frame, pos[0], pos[1], monitor["left"], monitor["top"], scale_x, scale_y
                    )
            writer.append_data(frame)
            frames += 1
            reste = frame_duration - (time.time() - t0)
            if reste > 0:
                time.sleep(reste)
    except KeyboardInterrupt:
        pass
    finally:
        writer.close()

    if not os.path.exists(tmp) or os.path.getsize(tmp) == 0:
        print(json.dumps({"ok": False, "error": "fichier vidéo vide"}))
        return 1
    if os.path.exists(out):
        os.remove(out)
    os.replace(tmp, out)
    duree = frames / fps
    print(
        json.dumps(
            {
                "ok": True,
                "path": out,
                "frames": frames,
                "duration_s": round(duree, 2),
                "fps": fps,
                "resolution": [target_w, target_h],
            }
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
