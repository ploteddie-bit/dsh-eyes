use serde_json::{json, Value};
use std::env;
use std::ffi::{c_char, c_int, c_void, CStr};
use std::process::ExitCode;
use std::time::Instant;

#[repr(C)]
struct AtspiAccessible {
    _prive: [u8; 0],
}

#[repr(C)]
struct AtspiComponent {
    _prive: [u8; 0],
}

#[repr(C)]
struct AtspiStateSet {
    _prive: [u8; 0],
}

#[repr(C)]
struct AtspiRect {
    x: i32,
    y: i32,
    largeur: i32,
    hauteur: i32,
}

#[link(name = "atspi")]
unsafe extern "C" {
    fn atspi_init() -> c_int;
    fn atspi_exit() -> c_int;
    fn atspi_get_desktop(index: c_int) -> *mut AtspiAccessible;
    fn atspi_accessible_get_name(obj: *mut AtspiAccessible, erreur: *mut c_void) -> *mut c_char;
    fn atspi_accessible_get_role_name(
        obj: *mut AtspiAccessible,
        erreur: *mut c_void,
    ) -> *mut c_char;
    fn atspi_accessible_get_child_count(obj: *mut AtspiAccessible, erreur: *mut c_void) -> c_int;
    fn atspi_accessible_get_child_at_index(
        obj: *mut AtspiAccessible,
        index: c_int,
        erreur: *mut c_void,
    ) -> *mut AtspiAccessible;
    fn atspi_accessible_get_process_id(obj: *mut AtspiAccessible, erreur: *mut c_void) -> u32;
    fn atspi_accessible_is_component(obj: *mut AtspiAccessible) -> c_int;
    fn atspi_accessible_get_component_iface(obj: *mut AtspiAccessible) -> *mut AtspiComponent;
    fn atspi_accessible_get_state_set(obj: *mut AtspiAccessible) -> *mut AtspiStateSet;
    fn atspi_component_get_extents(
        obj: *mut AtspiComponent,
        coordonnees: c_int,
        erreur: *mut c_void,
    ) -> *mut AtspiRect;
    fn atspi_rect_get_type() -> usize;
    fn atspi_state_set_contains(set: *mut AtspiStateSet, etat: c_int) -> c_int;
}

#[link(name = "glib-2.0")]
unsafe extern "C" {
    fn g_free(memoire: *mut c_void);
    fn g_boxed_free(type_boxed: usize, memoire: *mut c_void);
}

#[link(name = "gobject-2.0")]
unsafe extern "C" {
    fn g_object_unref(objet: *mut c_void);
}

struct Options {
    application: Option<String>,
    texte: Option<String>,
    limite: usize,
}

struct Collecteur {
    elements: Vec<Value>,
    texte: Option<String>,
    limite: usize,
}

fn options() -> Result<Options, String> {
    let mut resultat = Options {
        application: None,
        texte: None,
        limite: 200,
    };
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        let valeur = args.next().ok_or_else(|| format!("Valeur absente pour {arg}"))?;
        match arg.as_str() {
            "--application" => resultat.application = Some(valeur.to_lowercase()),
            "--texte" => resultat.texte = Some(valeur.to_lowercase()),
            "--limite" => {
                resultat.limite = valeur
                    .parse::<usize>()
                    .map_err(|_| "Limite invalide".to_string())?;
                if resultat.limite == 0 || resultat.limite > 2000 {
                    return Err("La limite doit être entre 1 et 2000".into());
                }
            }
            _ => return Err(format!("Option inconnue : {arg}")),
        }
    }
    Ok(resultat)
}

unsafe fn texte_libre(pointeur: *mut c_char) -> String {
    if pointeur.is_null() {
        return String::new();
    }
    let texte = unsafe { CStr::from_ptr(pointeur) }
        .to_string_lossy()
        .into_owned();
    unsafe { g_free(pointeur.cast()) };
    texte
}

unsafe fn etat(set: *mut AtspiStateSet, valeur: c_int) -> bool {
    !set.is_null() && unsafe { atspi_state_set_contains(set, valeur) } != 0
}

fn interactif(role: &str) -> bool {
    matches!(
        role,
        "button"
            | "push button"
            | "check box"
            | "radio button"
            | "combo box"
            | "entry"
            | "text"
            | "password text"
            | "link"
            | "list item"
            | "menu item"
            | "page tab"
    )
}

unsafe fn rectangle(obj: *mut AtspiAccessible, coordonnees: c_int) -> Option<Value> {
    if unsafe { atspi_accessible_is_component(obj) } == 0 {
        return None;
    }
    let composant = unsafe { atspi_accessible_get_component_iface(obj) };
    if composant.is_null() {
        return None;
    }
    let rect = unsafe { atspi_component_get_extents(composant, coordonnees, std::ptr::null_mut()) };
    let resultat = if rect.is_null() {
        None
    } else {
        let r = unsafe { &*rect };
        Some(json!({"x": r.x, "y": r.y, "largeur": r.largeur, "hauteur": r.hauteur}))
    };
    if !rect.is_null() {
        unsafe { g_boxed_free(atspi_rect_get_type(), rect.cast()) };
    }
    unsafe { g_object_unref(composant.cast()) };
    resultat
}

unsafe fn parcourir(
    obj: *mut AtspiAccessible,
    application: &str,
    pid: u32,
    profondeur: usize,
    collecteur: &mut Collecteur,
) {
    if obj.is_null() || profondeur > 12 || collecteur.elements.len() >= collecteur.limite {
        return;
    }
    let nom = unsafe { texte_libre(atspi_accessible_get_name(obj, std::ptr::null_mut())) };
    let role = unsafe { texte_libre(atspi_accessible_get_role_name(obj, std::ptr::null_mut())) };
    let correspond = collecteur
        .texte
        .as_ref()
        .is_none_or(|texte| nom.to_lowercase().contains(texte));
    if correspond && (!nom.is_empty() || interactif(&role)) {
        let set = unsafe { atspi_accessible_get_state_set(obj) };
        let actif = unsafe { etat(set, 8) };
        let visible = unsafe { etat(set, 30) };
        let montre = unsafe { etat(set, 25) };
        if !set.is_null() {
            unsafe { g_object_unref(set.cast()) };
        }
        collecteur.elements.push(json!({
            "application": application,
            "pid": pid,
            "nom": nom,
            "role": role,
            "actif": actif,
            "hors_ecran": !(visible && montre),
            "profondeur": profondeur,
            "rectangle_fenetre": unsafe { rectangle(obj, 1) },
            "rectangle_ecran_linux": unsafe { rectangle(obj, 0) },
        }));
    }

    let nombre = unsafe { atspi_accessible_get_child_count(obj, std::ptr::null_mut()) };
    for index in 0..nombre.max(0).min(collecteur.limite as c_int) {
        if collecteur.elements.len() >= collecteur.limite {
            break;
        }
        let enfant = unsafe {
            atspi_accessible_get_child_at_index(obj, index, std::ptr::null_mut())
        };
        if !enfant.is_null() {
            unsafe { parcourir(enfant, application, pid, profondeur + 1, collecteur) };
            unsafe { g_object_unref(enfant.cast()) };
        }
    }
}

fn executer(options: Options) -> Result<Value, String> {
    if unsafe { atspi_init() } > 1 {
        return Err("Initialisation AT-SPI impossible".into());
    }
    let debut = Instant::now();
    let mut applications = Vec::new();
    let desktop = unsafe { atspi_get_desktop(0) };
    if desktop.is_null() {
        unsafe { atspi_exit() };
        return Err("Bureau AT-SPI introuvable".into());
    }
    let nombre = unsafe { atspi_accessible_get_child_count(desktop, std::ptr::null_mut()) };
    let mut reste = options.limite;
    for index in 0..nombre.max(0) {
        if reste == 0 {
            break;
        }
        let application = unsafe {
            atspi_accessible_get_child_at_index(desktop, index, std::ptr::null_mut())
        };
        if application.is_null() {
            continue;
        }
        let nom = unsafe { texte_libre(atspi_accessible_get_name(application, std::ptr::null_mut())) };
        if options
            .application
            .as_ref()
            .is_some_and(|filtre| !nom.to_lowercase().contains(filtre))
        {
            unsafe { g_object_unref(application.cast()) };
            continue;
        }
        let pid = unsafe { atspi_accessible_get_process_id(application, std::ptr::null_mut()) };
        let mut collecteur = Collecteur {
            elements: Vec::new(),
            texte: options.texte.clone(),
            limite: reste,
        };
        unsafe { parcourir(application, &nom, pid, 0, &mut collecteur) };
        reste = reste.saturating_sub(collecteur.elements.len());
        applications.push(json!({
            "application": nom,
            "pid": pid,
            "elements": collecteur.elements,
        }));
        unsafe { g_object_unref(application.cast()) };
    }
    unsafe {
        g_object_unref(desktop.cast());
        atspi_exit();
    }
    Ok(json!({
        "applications": applications,
        "duree_ms": debut.elapsed().as_secs_f64() * 1000.0,
    }))
}

fn main() -> ExitCode {
    let resultat = options().and_then(executer);
    match resultat {
        Ok(valeur) => {
            println!("{}", serde_json::to_string_pretty(&valeur).unwrap_or_else(|_| "{}".into()));
            ExitCode::SUCCESS
        }
        Err(erreur) => {
            eprintln!("atspi-lire: {erreur}");
            ExitCode::FAILURE
        }
    }
}
