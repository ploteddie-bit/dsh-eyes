#!/usr/bin/env bash
# capture-ecran.sh — capture l'ÉCRAN PHYSIQUE Windows (pas l'intérieur du
# navigateur) avec le CURSEUR dessiné. Idée d'Eddie (20/10) : ne plus faire
# confiance à Chromium (captureVisibleTab mentait : images en cache) et voir
# la VÉRITÉ : fenêtre au premier plan, focus, position réelle du pointeur,
# bandeau debugger.
# Sortie : /mnt/c/Users/eddie/Downloads/ecran-dsh.png (+ echo du chemin)
set -eu
PS=/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe
OUT='C:\Users\eddie\Downloads\ecran-dsh.png'

"$PS" -NoProfile -Command '& {
  Add-Type -AssemblyName System.Windows.Forms
  Add-Type -AssemblyName System.Drawing
  Add-Type -TypeDefinition "using System; using System.Runtime.InteropServices; public struct PT { public int X; public int Y; } public class U32X { [DllImport(`"user32.dll`")] public static extern bool GetCursorPos(out PT p); }"
  $v = [System.Windows.Forms.SystemInformation]::VirtualScreen
  $bmp = New-Object System.Drawing.Bitmap($v.Width, $v.Height)
  $g = [System.Drawing.Graphics]::FromImage($bmp)
  $g.CopyFromScreen($v.X, $v.Y, 0, 0, $bmp.Size)
  $pt = New-Object PT
  [U32X]::GetCursorPos([ref]$pt) | Out-Null
  $cur = [System.Windows.Forms.Cursor]::Current
  $cur.Draw($g, (New-Object System.Drawing.Rectangle(($pt.X - $v.X), ($pt.Y - $v.Y), $cur.Size.Width, $cur.Size.Height)))
  $bmp.Save($args[0], [System.Drawing.Imaging.ImageFormat]::Png)
  "OK $($bmp.Width)x$($bmp.Height) curseur@($($pt.X),$($pt.Y))"
}' "$OUT"
echo "/mnt/c/Users/eddie/Downloads/ecran-dsh.png"
