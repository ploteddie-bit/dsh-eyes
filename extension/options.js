// options.js — configuration du token DSH Eyes dans chrome.storage.local.

const input = document.getElementById("token");
const status = document.getElementById("status");

chrome.storage.local.get({ dsh_token: "" }, (v) => {
  input.value = v.dsh_token || "";
});

document.getElementById("save").addEventListener("click", () => {
  const token = input.value.trim();
  chrome.storage.local.set({ dsh_token: token }, () => {
    if (token) {
      status.textContent = "Token enregistré — reconnexion en cours.";
      status.style.color = "#3fb950";
    } else {
      status.textContent = "Token vide — la connexion restera inactive.";
      status.style.color = "#d29922";
    }
  });
});

document.getElementById("show").addEventListener("click", () => {
  input.type = input.type === "password" ? "text" : "password";
});

// Import ponctuel depuis une URL (déploiement automatisé) :
// chrome-extension://<id>/options.html?token=<token>
const params = new URLSearchParams(location.search);
const provided = params.get("token");
if (provided) {
  const token = provided.trim();
  chrome.storage.local.set({ dsh_token: token }, () => {
    status.textContent = "Token importé — reconnexion en cours.";
    status.style.color = "#3fb950";
    history.replaceState({}, "", location.pathname);
  });
}
