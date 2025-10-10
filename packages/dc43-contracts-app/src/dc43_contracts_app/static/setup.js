const root = document.getElementById("setup-root");

if (!root) {
  console.warn("Setup wizard root element not found; skipping initialisation.");
} else {
  const stateEl = document.getElementById("setup-state");
  let setupState = {
    order: [],
    selected: {},
    configuration: {},
    modules: {},
    groups: [],
  };

  if (stateEl) {
    try {
      const parsed = JSON.parse(stateEl.textContent || "{}");
      if (Array.isArray(parsed.order)) {
        setupState.order = parsed.order.map((value) => String(value));
      }
      if (parsed.selected && typeof parsed.selected === "object") {
        setupState.selected = { ...parsed.selected };
      }
      if (parsed.configuration && typeof parsed.configuration === "object") {
        setupState.configuration = { ...parsed.configuration };
      }
      if (parsed.modules && typeof parsed.modules === "object") {
        setupState.modules = parsed.modules;
      }
      if (Array.isArray(parsed.groups)) {
        setupState.groups = parsed.groups;
      }
    } catch (error) {
      console.warn("Unable to parse setup wizard state", error);
    }
  }

  const moduleNodeMap = {
    authentication: { id: "auth", className: "security" },
    user_interface: { id: "ui", className: "interface" },
    ui_deployment: { id: "ui_dep", className: "deployment" },
    governance_service: { id: "gov", className: "runtime" },
    governance_deployment: { id: "gov_dep", className: "deployment" },
    governance_extensions: { id: "ext", className: "runtime" },
    contracts_backend: { id: "contracts", className: "storage" },
    products_backend: { id: "products", className: "storage" },
    data_quality: { id: "dq", className: "runtime" },
  };

  const step = Number.parseInt(root.getAttribute("data-current-step") || "1", 10);
  const mermaidContainer = root.querySelector("[data-setup-diagram]");
  const wizardSections = Array.from(root.querySelectorAll("[data-module-section]"));
  const wizardNavButtons = Array.from(root.querySelectorAll("[data-module-target]"));
  const wizardControls = root.querySelector("[data-wizard-controls]");
  const wizardPrev = root.querySelector("[data-wizard-prev]");
  const wizardNext = root.querySelector("[data-wizard-next]");
  const wizardProgress = root.querySelector("[data-wizard-progress]");
  const stepOneContainer = root.querySelector("[data-step1-wizard]");
  const stepOneSections = stepOneContainer
    ? Array.from(stepOneContainer.querySelectorAll("[data-step1-section]"))
    : [];
  const stepOneNavButtons = stepOneContainer
    ? Array.from(stepOneContainer.querySelectorAll("[data-step1-nav]"))
    : [];
  const stepOnePrev = stepOneContainer ? stepOneContainer.querySelector("[data-step1-prev]") : null;
  const stepOneNext = stepOneContainer ? stepOneContainer.querySelector("[data-step1-next]") : null;
  const stepOneProgress = stepOneContainer ? stepOneContainer.querySelector("[data-step1-progress]") : null;

  const selectedModuleKeys = setupState.order.filter((key) => Boolean(setupState.selected[key]));
  let activeModuleKey = selectedModuleKeys[0] || setupState.order[0] || null;
  let activeGroupKey = null;
  let diagramCounter = 0;

  function ensureConfiguration(moduleKey) {
    if (!setupState.configuration[moduleKey] || typeof setupState.configuration[moduleKey] !== "object") {
      setupState.configuration[moduleKey] = {};
    }
    return setupState.configuration[moduleKey];
  }

  function safeFocus(element) {
    if (!element || typeof element.focus !== "function") {
      return;
    }
    try {
      element.focus({ preventScroll: true });
    } catch (error) {
      element.focus();
    }
  }

  function sanitizeLabel(text) {
    const value = String(text ?? "");
    return value
      .replace(/[\r\t]+/g, " ")
      .split("\n")
      .map((line) => line.trim())
      .filter(Boolean)
      .map((line) => line.replace(/"/g, "'"))
      .join("\\n");
  }

  function moduleOptionMeta(moduleKey) {
    const moduleMeta = setupState.modules?.[moduleKey];
    if (!moduleMeta) {
      return null;
    }
    const optionKey = setupState.selected?.[moduleKey];
    if (!optionKey) {
      const options = moduleMeta.options || {};
      const entries = Object.entries(options);
      if (entries.length === 1) {
        const [, onlyMeta] = entries[0];
        return { module: moduleMeta, option: onlyMeta || null };
      }
      return { module: moduleMeta, option: null };
    }
    const optionMeta = moduleMeta.options?.[optionKey];
    return { module: moduleMeta, option: optionMeta || null };
  }

  function buildNodeLabel(moduleKey, includeDetails) {
    const meta = moduleOptionMeta(moduleKey);
    if (!meta) {
      return sanitizeLabel(moduleKey);
    }
    const { module, option } = meta;
    let label = module.title || moduleKey;
    if (option && option.label) {
      label += `\\n${option.label}`;
    } else {
      label += "\\nNot selected";
    }
    if (includeDetails && option && Array.isArray(option.fields)) {
      const configuration = ensureConfiguration(moduleKey);
      const details = [];
      for (const field of option.fields) {
        if (!field || !field.name) {
          continue;
        }
        const rawValue = configuration[field.name];
        if (!rawValue) {
          continue;
        }
        const valueText = String(rawValue).split("\n")[0];
        const labelText = field.label || field.name;
        details.push(`${labelText}: ${valueText}`);
        if (details.length >= 3) {
          break;
        }
      }
      if (details.length) {
        label += `\\n${details.map((item) => item.replace(/"/g, "'")).join("\\n")}`;
      }
    }
    return sanitizeLabel(label);
  }

  function buildMermaidDefinition(highlightKey) {
    const includeDetails = step >= 2;
    const nodes = {
      auth: buildNodeLabel("authentication", includeDetails),
      ui: buildNodeLabel("user_interface", includeDetails),
      ui_dep: buildNodeLabel("ui_deployment", includeDetails),
      gov: buildNodeLabel("governance_service", includeDetails),
      gov_dep: buildNodeLabel("governance_deployment", includeDetails),
      ext: buildNodeLabel("governance_extensions", includeDetails),
      contracts: buildNodeLabel("contracts_backend", includeDetails),
      products: buildNodeLabel("products_backend", includeDetails),
      dq: buildNodeLabel("data_quality", includeDetails),
    };

    const hasModule = (moduleKey) => {
      const moduleMeta = setupState.modules?.[moduleKey];
      if (!moduleMeta) {
        return false;
      }
      const options = moduleMeta.options || {};
      const optionKeys = Object.keys(options);
      if (!optionKeys.length || optionKeys.length === 1) {
        return true;
      }
      return Boolean(setupState.selected?.[moduleKey]);
    };
    const lines = [
      "flowchart LR",
      "  classDef default fill:#f8f9fa,stroke:#6c757d,stroke-width:1px,color:#212529;",
      "  classDef highlight fill:#fff3cd,stroke:#d39e00,stroke-width:2px,color:#212529;",
      "  classDef storage fill:#e3f2fd,stroke:#0d6efd,color:#0d6efd;",
      "  classDef runtime fill:#fdf2e9,stroke:#fd7e14,color:#d9480f;",
      "  classDef interface fill:#e2f0d9,stroke:#198754,color:#116530;",
      "  classDef security fill:#e7e9f9,stroke:#6f42c1,color:#3d2c8d;",
      "  classDef deployment fill:#fcefee,stroke:#d63384,color:#a61e4d;",
    ];

    const definedNodes = new Set();

    function defineNode(moduleKey, indent = "    ") {
      const nodeMeta = moduleNodeMap[moduleKey];
      if (!nodeMeta || !hasModule(moduleKey)) {
        return;
      }
      const label = nodes[nodeMeta.id];
      lines.push(`${indent}${nodeMeta.id}["${label}"]`);
      definedNodes.add(moduleKey);
    }

    const pushSubgraph = (title, moduleKeys) => {
      const active = moduleKeys.filter((key) => hasModule(key));
      if (!active.length) {
        return;
      }
      lines.push(`  subgraph "${sanitizeLabel(title)}"`);
      lines.push("    direction TB");
      for (const moduleKey of active) {
        defineNode(moduleKey);
      }
      lines.push("  end");
    };

    pushSubgraph("Interface", ["user_interface", "authentication"]);
    pushSubgraph("Deployments", ["ui_deployment", "governance_deployment"]);
    pushSubgraph("Governance", ["governance_service", "governance_extensions", "data_quality"]);
    pushSubgraph("Storage", ["contracts_backend", "products_backend"]);

    if (hasModule("user_interface") && hasModule("governance_service")) {
      lines.push("  ui -->|Orchestrates| gov");
    }
    if (hasModule("authentication") && hasModule("user_interface")) {
      lines.push("  auth -->|Protects| ui");
    }
    if (hasModule("governance_service") && hasModule("contracts_backend")) {
      lines.push("  gov -->|Publishes & reads| contracts");
    }
    if (hasModule("governance_service") && hasModule("products_backend")) {
      lines.push("  gov -->|Promotes| products");
    }
    if (hasModule("governance_service") && hasModule("data_quality")) {
      lines.push("  gov -->|Schedules| dq");
    }
    if (hasModule("governance_service") && hasModule("governance_extensions")) {
      lines.push("  gov -->|Extends via| ext");
    }
    if (hasModule("ui_deployment") && hasModule("user_interface")) {
      lines.push("  ui_dep -.->|Hosts| ui");
    }
    if (hasModule("governance_deployment") && hasModule("governance_service")) {
      lines.push("  gov_dep -.->|Hosts| gov");
    }

    for (const [moduleKey, nodeMeta] of Object.entries(moduleNodeMap)) {
      if (!definedNodes.has(moduleKey) || !nodeMeta.className) {
        continue;
      }
      lines.push(`  class ${nodeMeta.id} ${nodeMeta.className};`);
    }

    if (highlightKey && definedNodes.has(highlightKey)) {
      const node = moduleNodeMap[highlightKey];
      lines.push(`  class ${node.id} highlight;`);
    }

    return lines.join("\n");
  }

  function getGroupModuleKeys(groupKey) {
    if (!groupKey || !Array.isArray(setupState.groups)) {
      return [];
    }
    const groupEntry = setupState.groups.find((group) => group && group.key === groupKey);
    if (!groupEntry) {
      return [];
    }
    const keys = Array.isArray(groupEntry.modules) ? groupEntry.modules : [];
    return keys.map((value) => String(value));
  }

  function waitForMermaid() {
    if (window.mermaid && typeof window.mermaid.render === "function") {
      return Promise.resolve(window.mermaid);
    }
    return new Promise((resolve) => {
      let attempts = 0;
      const interval = setInterval(() => {
        attempts += 1;
        if (window.mermaid && typeof window.mermaid.render === "function") {
          clearInterval(interval);
          resolve(window.mermaid);
        } else if (attempts > 40) {
          clearInterval(interval);
          resolve(null);
        }
      }, 50);
    });
  }

  const mermaidReady = waitForMermaid();

  async function renderDiagram(highlightKey) {
    if (!mermaidContainer) {
      return;
    }
    const mermaid = await mermaidReady;
    if (!mermaid) {
      mermaidContainer.innerHTML = '<div class="text-danger small">Mermaid could not be loaded.</div>';
      return;
    }
    const definition = buildMermaidDefinition(highlightKey);
    diagramCounter += 1;
    try {
      const { svg, bindFunctions } = await mermaid.render(`setupDiagram${diagramCounter}`, definition);
      mermaidContainer.innerHTML = svg;
      if (typeof bindFunctions === "function") {
        bindFunctions(mermaidContainer);
      }
    } catch (error) {
      console.error("Failed to render setup diagram", error);
      mermaidContainer.innerHTML = '<div class="text-danger small">Unable to render architecture diagram.</div>';
    }
  }

  function setActiveModule(moduleKey, options = {}) {
    if (!moduleKey) {
      return;
    }
    activeModuleKey = moduleKey;
    updateWizardVisibility(options);
    updateWizardNav();
    renderDiagram(moduleKey);
  }

  function updateStepOneControls(currentIndex, total) {
    if (stepOneProgress) {
      if (currentIndex >= 0 && total > 0) {
        stepOneProgress.textContent = `Section ${currentIndex + 1} of ${total}`;
      } else {
        stepOneProgress.textContent = "";
      }
    }

    if (stepOnePrev) {
      stepOnePrev.disabled = currentIndex <= 0;
    }

    if (stepOneNext) {
      if (total <= 1) {
        stepOneNext.disabled = total === 0;
      } else {
        stepOneNext.disabled = false;
      }
      if (currentIndex === -1 || currentIndex >= total - 1) {
        stepOneNext.textContent = "Review selections";
      } else {
        stepOneNext.textContent = "Next section";
      }
    }
  }

  function setActiveGroup(groupKey, options = {}) {
    if (!groupKey || !stepOneSections.length) {
      return;
    }

    const groupKeys = stepOneSections
      .map((section) => section.getAttribute("data-step1-section"))
      .filter(Boolean);

    if (!groupKeys.includes(groupKey)) {
      return;
    }

    activeGroupKey = groupKey;

    const scrollIntoView = Boolean(options.scrollIntoView);

    stepOneSections.forEach((section) => {
      const key = section.getAttribute("data-step1-section");
      if (key === groupKey) {
        section.classList.remove("d-none");
        section.removeAttribute("hidden");
        if (scrollIntoView) {
          section.scrollIntoView({ behavior: "smooth", block: "start" });
        }
      } else {
        section.classList.add("d-none");
        section.setAttribute("hidden", "hidden");
      }
    });

    stepOneNavButtons.forEach((button) => {
      const key = button.getAttribute("data-step1-nav");
      button.classList.toggle("active", key === groupKey);
      if (key === groupKey) {
        button.setAttribute("aria-current", "true");
      } else {
        button.removeAttribute("aria-current");
      }
    });

    const currentIndex = groupKeys.indexOf(groupKey);
    updateStepOneControls(currentIndex, groupKeys.length);

    const moduleKeys = getGroupModuleKeys(groupKey);
    let highlightKey = options.highlightKey || null;
    if (!highlightKey) {
      highlightKey = moduleKeys.find((key) => setupState.selected[key]);
    }
    if (!highlightKey) {
      highlightKey = moduleKeys[0] || null;
    }

    if (highlightKey) {
      setActiveModule(highlightKey, { scrollIntoView: false });
    } else {
      renderDiagram(activeModuleKey);
    }
  }

  function updateWizardVisibility(options = {}) {
    if (!wizardSections.length) {
      return;
    }
    const wizardKeys = wizardSections
      .map((section) => section.getAttribute("data-module-key"))
      .filter((key) => key && setupState.order.includes(key));

    if (!wizardKeys.length || wizardKeys.length === 1) {
      if (wizardControls) {
        wizardControls.classList.add("d-none");
      }
      wizardSections.forEach((section) => {
        section.classList.remove("d-none");
        section.removeAttribute("hidden");
      });
      return;
    }

    if (wizardControls) {
      wizardControls.classList.remove("d-none");
    }

    const currentKey = activeModuleKey && wizardKeys.includes(activeModuleKey) ? activeModuleKey : wizardKeys[0];
    const scrollIntoView = Boolean(options.scrollIntoView);
    const focusOnSection = Boolean(options.focus);

    wizardSections.forEach((section) => {
      const key = section.getAttribute("data-module-key");
      if (!key || !wizardKeys.includes(key)) {
        section.classList.remove("d-none");
        section.removeAttribute("hidden");
        return;
      }
      if (key === currentKey) {
        section.classList.remove("d-none");
        section.removeAttribute("hidden");
        if (scrollIntoView) {
          section.scrollIntoView({ behavior: "smooth", block: "start" });
        }
        if (focusOnSection) {
          const focusTarget = section.querySelector("input, textarea, select");
          if (focusTarget) {
            safeFocus(focusTarget);
          }
        }
      } else {
        section.classList.add("d-none");
        section.setAttribute("hidden", "hidden");
      }
    });
  }

  function updateWizardNav() {
    if (!wizardNavButtons.length) {
      return;
    }
    const wizardKeys = wizardNavButtons
      .map((button) => button.getAttribute("data-module-target"))
      .filter((key) => key && setupState.order.includes(key));

    const currentIndex = activeModuleKey ? wizardKeys.indexOf(activeModuleKey) : -1;
    wizardNavButtons.forEach((button) => {
      const key = button.getAttribute("data-module-target");
      button.classList.toggle("active", key === activeModuleKey);
    });

    if (wizardProgress) {
      if (currentIndex >= 0) {
        wizardProgress.textContent = `Section ${currentIndex + 1} of ${wizardKeys.length}`;
      } else {
        wizardProgress.textContent = "";
      }
    }

    if (wizardPrev) {
      wizardPrev.disabled = currentIndex <= 0;
    }
    if (wizardNext) {
      if (currentIndex === -1 || currentIndex >= wizardKeys.length - 1) {
        wizardNext.textContent = "Go to summary";
      } else {
        wizardNext.textContent = "Next section";
      }
    }
  }

  function bindStepOneInteractions() {
    const optionInputs = Array.from(root.querySelectorAll('input[type="radio"][name^="module__"]'));
    optionInputs.forEach((input) => {
      input.addEventListener("change", (event) => {
        const target = event.currentTarget;
        if (!(target instanceof HTMLInputElement)) {
          return;
        }
        const [_, moduleKey] = target.name.split("__");
        if (!moduleKey) {
          return;
        }
        setupState.selected[moduleKey] = target.value;
        setActiveModule(moduleKey);
      });
    });
  }

  function bindStepOneWizard() {
    if (!stepOneContainer || !stepOneSections.length) {
      return;
    }

    const groupKeys = stepOneSections
      .map((section) => section.getAttribute("data-step1-section"))
      .filter(Boolean);

    let initialGroupKey = groupKeys.find((groupKey) => {
      const moduleKeys = getGroupModuleKeys(groupKey);
      return moduleKeys.some((moduleKey) => setupState.selected[moduleKey]);
    });

    if (!initialGroupKey) {
      initialGroupKey = groupKeys[0] || null;
    }

    if (initialGroupKey) {
      setActiveGroup(initialGroupKey);
    }

    stepOneNavButtons.forEach((button) => {
      button.addEventListener("click", () => {
        const key = button.getAttribute("data-step1-nav");
        if (key) {
          setActiveGroup(key, { scrollIntoView: true });
        }
      });
    });

    if (stepOnePrev) {
      stepOnePrev.addEventListener("click", () => {
        const currentIndex = groupKeys.indexOf(activeGroupKey);
        if (currentIndex > 0) {
          setActiveGroup(groupKeys[currentIndex - 1], { scrollIntoView: true });
        }
      });
    }

    if (stepOneNext) {
      stepOneNext.addEventListener("click", () => {
        const currentIndex = groupKeys.indexOf(activeGroupKey);
        if (currentIndex >= 0 && currentIndex < groupKeys.length - 1) {
          setActiveGroup(groupKeys[currentIndex + 1], { scrollIntoView: true });
        } else {
          const continueButton = root.querySelector('form button[type="submit"].btn-primary');
          if (continueButton instanceof HTMLElement) {
            continueButton.scrollIntoView({ behavior: "smooth", block: "center" });
            safeFocus(continueButton);
          }
        }
      });
    }
  }

  function bindConfigurationInputs() {
    const configInputs = Array.from(root.querySelectorAll('[name^="config__"]'));
    configInputs.forEach((input) => {
      input.addEventListener("input", (event) => {
        const target = event.currentTarget;
        if (!(target instanceof HTMLInputElement) && !(target instanceof HTMLTextAreaElement)) {
          return;
        }
        const nameParts = target.name.split("__");
        if (nameParts.length < 3) {
          return;
        }
        const moduleKey = nameParts[1];
        const fieldName = nameParts.slice(2).join("__");
        if (!moduleKey || !fieldName) {
          return;
        }
        const configuration = ensureConfiguration(moduleKey);
        configuration[fieldName] = target.value;
        renderDiagram(activeModuleKey);
      });
    });
  }

  function bindWizardNav() {
    wizardNavButtons.forEach((button) => {
      button.addEventListener("click", () => {
        const key = button.getAttribute("data-module-target");
        if (!key) {
          return;
        }
        setActiveModule(key, { scrollIntoView: true, focus: true });
      });
    });

    wizardSections.forEach((section) => {
      section.addEventListener("focusin", () => {
        const key = section.getAttribute("data-module-key");
        if (key) {
          setActiveModule(key);
        }
      });
      section.addEventListener("mouseenter", () => {
        const key = section.getAttribute("data-module-key");
        if (key) {
          setActiveModule(key);
        }
      });
    });

    if (wizardPrev) {
      wizardPrev.addEventListener("click", () => {
        const wizardKeys = wizardNavButtons
          .map((button) => button.getAttribute("data-module-target"))
          .filter((key) => key && setupState.order.includes(key));
        const currentIndex = activeModuleKey ? wizardKeys.indexOf(activeModuleKey) : -1;
        if (currentIndex > 0) {
          setActiveModule(wizardKeys[currentIndex - 1], { scrollIntoView: true, focus: true });
        }
      });
    }

    if (wizardNext) {
      wizardNext.addEventListener("click", () => {
        const wizardKeys = wizardNavButtons
          .map((button) => button.getAttribute("data-module-target"))
          .filter((key) => key && setupState.order.includes(key));
        const currentIndex = activeModuleKey ? wizardKeys.indexOf(activeModuleKey) : -1;
        if (currentIndex >= 0 && currentIndex < wizardKeys.length - 1) {
          setActiveModule(wizardKeys[currentIndex + 1], { scrollIntoView: true, focus: true });
        } else {
          const summaryButton = root.querySelector('form button[type="submit"].btn-primary');
          if (summaryButton instanceof HTMLElement) {
            summaryButton.scrollIntoView({ behavior: "smooth", block: "center" });
            safeFocus(summaryButton);
          }
        }
      });
    }
  }

  bindStepOneInteractions();
  bindStepOneWizard();
  bindConfigurationInputs();
  bindWizardNav();
  updateWizardVisibility();
  updateWizardNav();
  renderDiagram(activeModuleKey);
}
