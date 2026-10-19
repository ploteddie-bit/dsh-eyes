#!/usr/bin/env bash
# os-clic.sh X Y — déplace le VRAI pointeur Windows vers (X,Y) écran avec une
# trajectoire en 6 pas, puis clic réel (mouse_event LEFTDOWN/UP avec dwell).
#
# Usage : os-clic.sh 1024 512
# Prérequis : la fenêtre Chrome cible est au premier plan (screen_pos active
# l'onglet juste avant). Le pointeur est PRIS EN CHARGE pendant ~300 ms.
# Calibrage : si le clic tombe à côté (zoom/DPI atypique), ajuster
# OS_OFFSET_X / OS_OFFSET_Y ci-dessous (en px écran).
set -eu
X="${1:?usage: os-clic.sh X Y}"
Y="${2:?usage: os-clic.sh X Y}"
OS_OFFSET_X="${OS_OFFSET_X:-0}"
OS_OFFSET_Y="${OS_OFFSET_Y:-0}"
PS=/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe

"$PS" -NoProfile -Command '& {
  Add-Type -AssemblyName System.Windows.Forms
  Add-Type -MemberDefinition "[DllImport(`"user32.dll`")] public static extern bool SetCursorPos(int x,int y); [DllImport(`"user32.dll`")] public static extern void mouse_event(uint f,uint dx,uint dy,uint d,UIntPtr e); [DllImport(`"user32.dll`")] public static extern bool SetForegroundWindow(IntPtr h); [DllImport(`"user32.dll`")] public static extern bool ShowWindow(IntPtr h, int c);" -Name U32 -Namespace W
  # v0.5.2 : amener Chrome au premier plan ET le restaurer si minimisé.
  # Constat : Chrome remonte parfois state≠minimized alors que la fenetre
  # est minimisee (geometrie absurde barresY<0 cote extension) - on restaure
  # cote WINDOWS, ShowWindow(9)=SW_RESTORE, inoffensif si deja visible.
  $c = Get-Process chrome -ErrorAction SilentlyContinue | Where-Object { $_.MainWindowHandle -ne 0 } | Select-Object -First 1
  if ($c) { [W.U32]::ShowWindow($c.MainWindowHandle, 9) | Out-Null; [W.U32]::SetForegroundWindow($c.MainWindowHandle) | Out-Null; Start-Sleep -Milliseconds 350 }
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
