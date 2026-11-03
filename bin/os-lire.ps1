param(
    [string]$Titre = "",
    [string]$Processus = "",
    [string]$Texte = "",
    [int]$Limite = 200
)

$ErrorActionPreference = "Stop"
Add-Type -AssemblyName UIAutomationClient
Add-Type -AssemblyName UIAutomationTypes

$handle = [IntPtr]::Zero
$process = $null
if ($Processus) {
    $process = Get-Process -Name $Processus -ErrorAction SilentlyContinue |
        Where-Object { $_.MainWindowHandle -ne 0 } |
        Select-Object -First 1
} elseif ($Titre) {
    $process = Get-Process -ErrorAction SilentlyContinue |
        Where-Object { $_.MainWindowTitle -like "*$Titre*" } |
        Select-Object -First 1
}
if ($process) {
    $handle = $process.MainWindowHandle
} else {
    Add-Type -TypeDefinition 'using System; using System.Runtime.InteropServices; public static class FenetreActive { [DllImport("user32.dll")] public static extern IntPtr GetForegroundWindow(); }'
    $handle = [FenetreActive]::GetForegroundWindow()
}
if ($handle -eq [IntPtr]::Zero) {
    throw "Fenêtre native introuvable"
}

$racine = [System.Windows.Automation.AutomationElement]::FromHandle($handle)
$marcheur = [System.Windows.Automation.TreeWalker]::ControlViewWalker
$liste = [System.Collections.Generic.List[object]]::new()
$roles = @("Button", "CheckBox", "ComboBox", "Edit", "Hyperlink", "ListItem", "MenuItem", "TabItem")

function Lire-Arbre {
    param($Element, $Profondeur)
    if ($null -eq $Element -or $Profondeur -gt 12 -or $liste.Count -ge $Limite) {
        return
    }
    $courant = $Element.Current
    $nom = [string]$courant.Name
    $role = [string]$courant.ControlType.ProgrammaticName
    $interactif = $roles | Where-Object { $role.EndsWith(".$_") }
    if (($nom -or $interactif) -and (-not $Texte -or $nom -like "*$Texte*")) {
        $rect = $courant.BoundingRectangle
        $liste.Add([PSCustomObject]@{
            nom = $nom
            role = $role
            actif = [bool]$courant.IsEnabled
            hors_ecran = [bool]$courant.IsOffscreen
            x = [math]::Round($rect.X)
            y = [math]::Round($rect.Y)
            largeur = [math]::Round($rect.Width)
            hauteur = [math]::Round($rect.Height)
        })
    }
    $enfant = $marcheur.GetFirstChild($Element)
    while ($enfant -and $liste.Count -lt $Limite) {
        Lire-Arbre $enfant ($Profondeur + 1)
        $enfant = $marcheur.GetNextSibling($enfant)
    }
}

Lire-Arbre $racine 0
[PSCustomObject]@{
    titre = $racine.Current.Name
    pid = $racine.Current.ProcessId
    elements = @($liste)
} | ConvertTo-Json -Depth 5 -Compress
