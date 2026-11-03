---
name: installation-extensions-m2m
description: Installer et vérifier DSH Eyes et Agent achat M2M dans le Chrome Windows d’Eddie, avec enregistrement Native Messaging, configuration du token DSH Eyes et preuves fonctionnelles réelles. À utiliser pour toute installation, réparation ou contrôle de ces deux extensions.
metadata:
  version: "1.0.0"
---

# Installation et conformité — DSH Eyes + Agent achat M2M

Objectif : prouver une installation Windows complète, pas seulement la présence des fichiers ou des extensions dans `chrome://extensions`.

**Jamais de secret dans la conversation, les captures ou les journaux. Ne pas lire `~/.dsh-eyes/token` avec un outil ou une commande. La saisie du token DSH Eyes est manuelle.**

## 1. Préparer les deux extensions

Sources :

- DSH Eyes : `Edy:~/projets/dsh-eyes/extension/` — version attendue `1.2.0`.
- Agent M2M : `Edy:~/projets/agent-achat-m2m/extension/` — version attendue `0.4.0`.

Destination Windows :

```text
C:\Users\eddie\Documents\AAA\dsh-eyes-extension
C:\Users\eddie\Documents\AAA\agent-achat-m2m-extension-0.4-test-20261102
```

Avant tout remplacement, conserver une sauvegarde dans `versions/`. Copier les fichiers sans supprimer les autres documents, puis vérifier :

```bash
node --check /mnt/c/Users/eddie/Documents/AAA/dsh-eyes-extension/background.js
node --check /mnt/c/Users/eddie/Documents/AAA/agent-achat-m2m-extension-0.4-test-20261102/service-worker.js
node --check /mnt/c/Users/eddie/Documents/AAA/agent-achat-m2m-extension-0.4-test-20261102/panneau.js
```

Dans `chrome://extensions`, activer le mode développeur, charger les deux dossiers comme extensions non empaquetées et relever :

- DSH Eyes `1.2.0`, ID actuel `mmbaefkcnnppiiocdilpglefdkhcalcj` ;
- Agent achat M2M `0.4.0`, ID `pepieejonckogakfikjdbkamgieojijn`.

Une version affichée et un bouton actif ne prouvent pas encore le fonctionnement.

## 2. Installer Agent M2M sous Windows

L’installateur actuel est :

```text
Edy:~/projets/agent-achat-m2m/target/x86_64-pc-windows-gnu/release/bundle/nsis/Agent achat M2M_0.1.0_x64-setup.exe
```

Relever son SHA-256 avant exécution. S’il est absent, le reconstruire avec la procédure Rust/Tauri du projet, sans télécharger un paquet ailleurs.

Lancer l’installateur via PowerShell :

```powershell
$setup = "\\wsl.localhost\Ubuntu-26.04\home\eddie\projets\agent-achat-m2m\target\x86_64-pc-windows-gnu\release\bundle\nsis\Agent achat M2M_0.1.0_x64-setup.exe"
Start-Process -FilePath $setup -ArgumentList "/S" -Wait
```

Windows peut afficher un avertissement car le paquet n’est pas signé. Ne poursuivre qu’avec le paquet reconstruit et son hash relevé.

Localiser puis lancer l’application installée :

```powershell
$app = Get-ItemProperty "HKCU:\Software\Microsoft\Windows\CurrentVersion\Uninstall\*" |
  Where-Object { $_.DisplayName -eq "Agent achat M2M" } |
  Select-Object -First 1
Start-Process -FilePath $app.DisplayIcon
```

Le premier lancement crée le manifeste et la clé Native Messaging. **Ne pas utiliser le binaire Tauri WSL pour valider Chrome Windows** : il ne peut pas remplacer l’enregistrement Windows.

Vérifier la clé :

```powershell
Get-ItemProperty "HKCU:\Software\Google\Chrome\NativeMessagingHosts\fr.annonces_m2m.agent_achat"
```

Puis lire le manifeste affiché. Il doit contenir :

```json
{
  "name": "fr.annonces_m2m.agent_achat",
  "type": "stdio",
  "allowed_origins": ["chrome-extension://pepieejonckogakfikjdbkamgieojijn/"]
}
```

Le `path` doit pointer vers un exécutable Windows existant. Laisser l’application ouverte pendant le test.

## 3. Configurer DSH Eyes

Le démon doit être actif :

```bash
systemctl --user is-active dsh-eyes
curl -s http://127.0.0.1:8445/health
```

Ouvrir dans Chrome :

```text
chrome-extension://mmbaefkcnnppiiocdilpglefdkhcalcj/options.html
```

**Configuration du token** : l’utilisateur colle le contenu de `Edy:~/.dsh-eyes/token` dans le champ Token, puis clique `Enregistrer`. Sur demande explicite de l’utilisateur, l’agent peut effectuer cette configuration assistée via le presse-papiers et les outils dédiés, sans afficher le token dans la conversation ou sur une capture.

Si `eyes_eval` répond `no_extension_connected`, vérifier d’abord que ce champ n’est pas vide et que le démon a reçu le handshake :

```bash
journalctl --user-unit dsh-eyes --since '5 minutes ago' --no-pager
```

Puis vérifier une vraie commande Chrome avec `eyes_ping` et `eyes_eval`. Un simple `eyes_ping` ne suffit pas : il peut interroger le démon sans prouver l’extension.

## 4. Vérifier Agent M2M dans Chrome

Ouvrir une nouvelle fenêtre pour éviter un ancien problème de focus :

```bash
/mnt/c/Windows/System32/cmd.exe /c start "" "C:\Program Files\Google\Chrome\Application\chrome.exe" --new-window "chrome-extension://pepieejonckogakfikjdbkamgieojijn/panneau.html"
```

Cliquer `Rechercher` ou lui donner le focus puis appuyer sur Entrée.

Preuve attendue : le panneau affiche `10 annonces — lecture seule` et de vraies annonces avec titres et prix. L’application Windows doit rester active et son serveur loopback doit écouter.

Si le message est `Application M2M indisponible` :

1. vérifier que l’application Windows est lancée ;
2. vérifier la clé Native Messaging ;
3. vérifier le `path` du manifeste et `allowed_origins` ;
4. vérifier que l’ID de l’extension n’a pas changé.

## 5. Critères de conformité finale

Ne déclarer la conformité que si les quatre preuves sont réunies :

- versions et IDs visibles dans `chrome://extensions` ;
- DSH Eyes exécute une commande Chrome réelle sans `no_extension_connected` ;
- Agent M2M affiche les annonces réelles via son hôte Windows ;
- aucun secret n’apparaît dans les captures, commandes ou journaux.

Le parcours de paiement est hors de ce skill : il relève d’une dette future distincte et ne doit pas être testé pendant l’installation.
