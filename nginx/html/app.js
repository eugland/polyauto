async function loadApps() {
  const grid = document.getElementById("apps-grid");
  const tpl = document.getElementById("app-card-template");

  try {
    const res = await fetch("/apps.json", { cache: "no-store" });
    if (!res.ok) {
      throw new Error("Could not read /apps.json");
    }
    const data = await res.json();
    const apps = Array.isArray(data.apps) ? data.apps : [];

    if (!apps.length) {
      grid.innerHTML = '<p class="empty">No apps listed yet. Add items in html/apps.json.</p>';
      return;
    }

    for (const app of apps) {
      const node = tpl.content.cloneNode(true);
      const route = app.route || "/";
      node.querySelector(".name").textContent = app.name || "unnamed";
      node.querySelector(".tag").textContent = app.kind || "service";
      node.querySelector(".desc").textContent = app.description || "No description provided.";
      node.querySelector(".route").textContent = route;
      node.querySelector(".route").href = route;
      node.querySelector(".target").textContent = `Upstream: ${app.target || "n/a"}`;
      grid.appendChild(node);
    }
  } catch (err) {
    grid.innerHTML = `<p class="empty">Failed to load apps list: ${err.message}</p>`;
  }
}

loadApps();
