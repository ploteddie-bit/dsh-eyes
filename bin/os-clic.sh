#!/usr/bin/env bash
# os-clic.sh X Y — déplace le VRAI pointeur Windows vers (X,Y) écran avec une
# trajectoire en 6 pas, puis clic réel (mouse_event LEFTDOWN/UP avec dwell).
#
# Usage : os-clic.sh 1024 512
# Prérequis : la fenêtre cible est au premier plan (screen_pos active
# l'onglet juste avant). Le pointeur est PRIS EN CHARGE pendant ~300 ms.
# OS_TITRE permet de cibler une autre fenêtre que Chrome ; OS_OFFSET_X / OS_OFFSET_Y
# ci-dessous (en px écran) permettent le calibrage zoom/DPI.
set -eu
X="${1:?usage: os-clic.sh X Y}"
Y="${2:?usage: os-clic.sh X Y}"
OS_OFFSET_X="${OS_OFFSET_X:-0}"
OS_OFFSET_Y="${OS_OFFSET_Y:-0}"
OS_TITRE="${OS_TITRE:-}"
PS=/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe
export WSLENV="${WSLENV:+$WSLENV:}OS_TITRE"

"$PS" -NoProfile -Command '& {
  Add-Type -AssemblyName System.Windows.Forms
  Add-Type -MemberDefinition "[DllImport(`"user32.dll`")] public static extern bool SetCursorPos(int x,int y); [DllImport(`"user32.dll`")] public static extern void mouse_event(uint f,uint dx,uint dy,uint d,UIntPtr e); [DllImport(`"user32.dll`")] public static extern bool SetForegroundWindow(IntPtr h); [DllImport(`"user32.dll`")] public static extern bool ShowWindow(IntPtr h, int c); [DllImport(`"user32.dll`")] public static extern IntPtr GetForegroundWindow(); [DllImport(`"user32.dll`")] public static extern bool IsIconic(IntPtr h);" -Name U32 -Namespace W
  # OS_TITRE cible une application native sans joker et sans ramener Chrome au premier plan.
  $c = if ($env:OS_TITRE) { Get-Process -ErrorAction SilentlyContinue | Where-Object { $_.MainWindowTitle.IndexOf($env:OS_TITRE, [StringComparison]::OrdinalIgnoreCase) -ge 0 } | Select-Object -First 1 } else { Get-Process chrome -ErrorAction SilentlyContinue | Where-Object { $_.MainWindowHandle -ne 0 } | Select-Object -First 1 }
  if (-not $c) { Write-Error "fenetre cible introuvable ; clic refuse" ; exit 3 }
  $h = $c.MainWindowHandle
  if ([W.U32]::IsIconic($h)) {
    [W.U32]::ShowWindow($h, 9) | Out-Null
    Write-Error "fenetre restauree ; refaire la capture avant de cliquer"
    exit 4
  }
  [W.U32]::ShowWindow($h, 9) | Out-Null
  [W.U32]::SetForegroundWindow($h) | Out-Null
  Start-Sleep -Milliseconds 350
  if ([W.U32]::GetForegroundWindow() -ne $h) { Write-Error "focus non confirme sur $($c.MainWindowTitle) ; clic refuse" ; exit 5 }
  $x=[int]$args[0]; $y=[int]$args[1]
  $p=[System.Windows.Forms.Cursor]::Position
  $n=6
  for($i=1;$i -le $n;$i++){
    $nx=[int]($p.X + ($x-$p.X)*$i/$n)
    $ny=[int]($p.Y + ($y-$p.Y)*$i/$n)
    [W.U32]::SetCursorPos($nx,$ny)|Out-Null
    Start-Sleep -Milliseconds (15 + (Get-Random -Maximum 25))
  }
  Start-Sleep -Milliseconds (50 + (Get-Random -Maximum 80))
  [W.U32]::mouse_event(2,0,0,0,[UIntPtr]::Zero)
  Start-Sleep -Milliseconds (45 + (Get-Random -Maximum 55))
  [W.U32]::mouse_event(4,0,0,0,[UIntPtr]::Zero)
  "CLIC OS @($x,$y)"
}' "$((X + OS_OFFSET_X))" "$((Y + OS_OFFSET_Y))"
