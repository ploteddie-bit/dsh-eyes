#!/usr/bin/env bash
# capture-ecran.sh — capture l'ÉCRAN PHYSIQUE Windows (pas l'intérieur du
# navigateur) avec le CURSEUR dessiné. Idée d'Eddie (20/10) : ne plus faire
# confiance à Chromium (captureVisibleTab mentait : images en cache) et voir
# la VÉRITÉ : fenêtre au premier plan, focus, position réelle du pointeur,
# bandeau debugger.
#
# Options :
#   --region x,y,w,h    capture une zone précise (pixels écran)
#   --fenetre [titre]   capture une fenêtre (titre partiel) ; sans titre = fenêtre active
#   --out <chemin>      fichier de sortie Windows (défaut : C:\Users\eddie\Downloads\ecran-dsh.png)
#
# Sortie : "OK LARGEURxHAUTEUR mode curseur@(x,y)" puis le chemin WSL.
set -eu
PS=/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe

OUT='C:\Users\eddie\Downloads\ecran-dsh.png'
REGION=''
TITRE=''
FENETRE=0

while [ $# -gt 0 ]; do
  case "$1" in
    --region)
      REGION="${2:-}"; shift 2 ;;
    --fenetre)
      FENETRE=1
      if [ $# -ge 2 ] && [ "${2#--}" = "$2" ]; then TITRE="$2"; shift 2; else shift; fi ;;
    --out)
      OUT="${2:?--out requiert un chemin}"; shift 2 ;;
    -h|--help)
      sed -n '2,13p' "$0"; exit 0 ;;
    *)
      echo "capture-ecran: option inconnue: $1" >&2; exit 2 ;;
  esac
done

RX=0; RY=0; RW=0; RH=0
if [ -n "$REGION" ]; then
  IFS=',' read -r RX RY RW RH <<< "$REGION"
  for v in "$RX" "$RY" "$RW" "$RH"; do
    case "$v" in ''|*[!0-9]*) echo "capture-ecran: --region attend x,y,w,h en entiers" >&2; exit 2;; esac
  done
  if [ "$RW" -le 0 ] || [ "$RH" -le 0 ]; then
    echo "capture-ecran: --region largeur et hauteur doivent être > 0" >&2
    exit 2
  fi
fi

export DSH_OUT="$OUT" DSH_RX="$RX" DSH_RY="$RY" DSH_RW="$RW" DSH_RH="$RH" DSH_TITRE="$TITRE" DSH_FENETRE="$FENETRE"
# WSL ne transmet aux exécutables Windows que les variables listées dans WSLENV.
export WSLENV="${WSLENV:+$WSLENV:}DSH_OUT:DSH_RX:DSH_RY:DSH_RW:DSH_RH:DSH_TITRE:DSH_FENETRE"

"$PS" -NoProfile -Command '& {
  Add-Type -AssemblyName System.Windows.Forms
  Add-Type -AssemblyName System.Drawing
  Add-Type -TypeDefinition @"
using System;
using System.Runtime.InteropServices;
public struct PT { public int X; public int Y; }
public struct RECT { public int Left; public int Top; public int Right; public int Bottom; }
public class U32X {
  [DllImport("user32.dll")] public static extern bool GetCursorPos(out PT p);
  [DllImport("user32.dll")] public static extern IntPtr GetForegroundWindow();
  [DllImport("user32.dll")] public static extern bool GetWindowRect(IntPtr h, out RECT r);
  [DllImport("user32.dll")] public static extern bool SetForegroundWindow(IntPtr h);
}
"@
  $out = $env:DSH_OUT
  $titre = $env:DSH_TITRE
  $fenetre = [int]$env:DSH_FENETRE
  $rx = [int]$env:DSH_RX; $ry = [int]$env:DSH_RY
  $rw = [int]$env:DSH_RW; $rh = [int]$env:DSH_RH
  $x = 0; $y = 0; $w = 0; $h = 0; $mode = "ecran"

  if ($fenetre -eq 1) {
    if ($titre -ne "") {
      $p = Get-Process | Where-Object {
        $_.MainWindowTitle -and $_.MainWindowTitle -like "*$titre*"
      } | Select-Object -First 1
      if (-not $p) { Write-Output "ERREUR fenetre introuvable: $titre"; exit 3 }
      $hwin = $p.MainWindowHandle
      $mode = "fenetre:$($p.MainWindowTitle)"
    } else {
      $hwin = [U32X]::GetForegroundWindow()
      $mode = "fenetre-active"
    }
    [U32X]::SetForegroundWindow($hwin) | Out-Null
    Start-Sleep -Milliseconds 250
    $r = New-Object RECT
    [U32X]::GetWindowRect($hwin, [ref]$r) | Out-Null
    $x = $r.Left; $y = $r.Top
    $w = $r.Right - $r.Left; $h = $r.Bottom - $r.Top
  } elseif ($rw -gt 0) {
    $x = $rx; $y = $ry; $w = $rw; $h = $rh; $mode = "region"
  } else {
    $v = [System.Windows.Forms.SystemInformation]::VirtualScreen
    $x = $v.X; $y = $v.Y; $w = $v.Width; $h = $v.Height
  }

  if ($w -le 0 -or $h -le 0) { Write-Output "ERREUR dimensions invalides: ${w}x${h}"; exit 4 }
  $bmp = New-Object System.Drawing.Bitmap($w, $h)
  $g = [System.Drawing.Graphics]::FromImage($bmp)
  $g.CopyFromScreen($x, $y, 0, 0, $bmp.Size)
  $pt = New-Object PT
  [U32X]::GetCursorPos([ref]$pt) | Out-Null
  $cur = [System.Windows.Forms.Cursor]::Current
  $cx = $pt.X - $x; $cy = $pt.Y - $y
  if ($cx -ge 0 -and $cy -ge 0 -and $cx -lt $w -and $cy -lt $h) {
    $cur.Draw($g, (New-Object System.Drawing.Rectangle($cx, $cy, $cur.Size.Width, $cur.Size.Height)))
  }
  $bmp.Save($out, [System.Drawing.Imaging.ImageFormat]::Png)
  "OK $($bmp.Width)x$($bmp.Height) $mode curseur@($($pt.X),$($pt.Y))"
}'

# Chemin WSL correspondant (C:\... → /mnt/c/...)
if [[ "$OUT" =~ ^[Cc]:\\ ]]; then
  WSL="/mnt/c/${OUT:3}"; WSL="${WSL//\\//}"
  echo "$WSL"
fi
