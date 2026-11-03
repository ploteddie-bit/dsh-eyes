using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using System.Web.Script.Serialization;
using System.Windows.Automation;

internal static class LecteurNatif
{
    [DllImport("user32.dll")]
    private static extern IntPtr GetForegroundWindow();

    private sealed class Options
    {
        internal string Titre = "";
        internal string Processus = "";
        internal string Texte = "";
        internal int Limite = 200;
    }

    private static Options LireOptions(string[] args)
    {
        var options = new Options();
        for (var i = 0; i < args.Length; i++)
        {
            if (i + 1 >= args.Length) throw new ArgumentException("Valeur absente pour " + args[i]);
            var valeur = args[++i];
            switch (args[i - 1])
            {
                case "--titre": options.Titre = valeur; break;
                case "--processus": options.Processus = valeur; break;
                case "--texte": options.Texte = valeur; break;
                case "--limite": options.Limite = int.Parse(valeur); break;
                default: throw new ArgumentException("Option inconnue : " + args[i - 1]);
            }
        }
        if (options.Limite < 1 || options.Limite > 2000) throw new ArgumentException("Limite hors bornes");
        return options;
    }

    private static IntPtr TrouverFenetre(Options options, out Process processus)
    {
        processus = null;
        Process[] candidats = Process.GetProcesses();
        foreach (var candidat in candidats)
        {
            if (candidat.MainWindowHandle == IntPtr.Zero) continue;
            if (options.Processus.Length > 0 && candidat.ProcessName == options.Processus)
            {
                processus = candidat;
                break;
            }
            if (options.Titre.Length > 0 && candidat.MainWindowTitle.Contains(options.Titre))
            {
                processus = candidat;
                break;
            }
        }
        return processus == null ? GetForegroundWindow() : processus.MainWindowHandle;
    }

    private static bool EstInteractif(ControlType role)
    {
        return role == ControlType.Button || role == ControlType.CheckBox ||
            role == ControlType.ComboBox || role == ControlType.Edit ||
            role == ControlType.Hyperlink || role == ControlType.ListItem ||
            role == ControlType.MenuItem || role == ControlType.TabItem;
    }

    private static void Main(string[] args)
    {
        Console.OutputEncoding = Encoding.UTF8;
        var options = LireOptions(args);
        Process processus;
        var handle = TrouverFenetre(options, out processus);
        if (handle == IntPtr.Zero) throw new InvalidOperationException("Fenêtre native introuvable");

        var racine = AutomationElement.FromHandle(handle);
        var resultat = new List<Dictionary<string, object>>();
        var file = new Queue<Tuple<AutomationElement, int>>();
        file.Enqueue(Tuple.Create(racine, 0));
        while (file.Count > 0 && resultat.Count < options.Limite)
        {
            var courant = file.Dequeue();
            var element = courant.Item1;
            var profondeur = courant.Item2;
            if (profondeur > 12) continue;
            var proprietes = element.Current;
            var nom = proprietes.Name ?? "";
            var role = proprietes.ControlType.ProgrammaticName;
            if ((nom.Length > 0 || EstInteractif(proprietes.ControlType)) &&
                (options.Texte.Length == 0 || nom.Contains(options.Texte)))
            {
                var rect = proprietes.BoundingRectangle;
                var ligne = new Dictionary<string, object>();
                ligne.Add("nom", nom);
                ligne.Add("role", role);
                ligne.Add("actif", proprietes.IsEnabled);
                ligne.Add("hors_ecran", proprietes.IsOffscreen);
                ligne.Add("x", Math.Round(rect.X));
                ligne.Add("y", Math.Round(rect.Y));
                ligne.Add("largeur", Math.Round(rect.Width));
                ligne.Add("hauteur", Math.Round(rect.Height));
                resultat.Add(ligne);
            }
            var enfant = TreeWalker.ControlViewWalker.GetFirstChild(element);
            while (enfant != null)
            {
                file.Enqueue(Tuple.Create(enfant, profondeur + 1));
                enfant = TreeWalker.ControlViewWalker.GetNextSibling(enfant);
            }
        }
        var sortie = new Dictionary<string, object>();
        sortie.Add("titre", racine.Current.Name);
        sortie.Add("pid", racine.Current.ProcessId);
        sortie.Add("processus", processus == null ? "" : processus.ProcessName);
        var fenetre = racine.Current.BoundingRectangle;
        sortie.Add("fenetre_x", Math.Round(fenetre.X));
        sortie.Add("fenetre_y", Math.Round(fenetre.Y));
        sortie.Add("elements", resultat);
        Console.WriteLine(new JavaScriptSerializer().Serialize(sortie));
    }
}
