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
    autoSelected: new Set(),
    explicitSelected: new Set(),
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
      if (Array.isArray(parsed.autoSelected)) {
        setupState.autoSelected = new Set(parsed.autoSelected.map((value) => String(value)));
      }
      if (Array.isArray(parsed.explicitSelected)) {
        setupState.explicitSelected = new Set(parsed.explicitSelected.map((value) => String(value)));
      }
    } catch (error) {
      console.warn("Unable to parse setup wizard state", error);
    }
  }

  if (!(setupState.autoSelected instanceof Set)) {
    const values = Array.isArray(setupState.autoSelected) ? setupState.autoSelected : [];
    setupState.autoSelected = new Set(values.map((value) => String(value)));
  }

  const moduleNodeMap = {
    authentication: { id: "auth", className: "security" },
    user_interface: { id: "ui", className: "interface" },
    ui_deployment: { id: "ui_dep", className: "deployment" },
    governance_service: { id: "gov", className: "runtime" },
    governance_deployment: { id: "gov_dep", className: "deployment" },
    governance_store: { id: "gov_store", className: "storage" },
    governance_extensions: { id: "ext", className: "runtime" },
    pipeline_integration: { id: "pipeline", className: "runtime" },
    contracts_backend: { id: "contracts", className: "storage" },
    products_backend: { id: "products", className: "storage" },
    data_quality: { id: "dq", className: "runtime" },
    demo_automation: { id: "demo", className: "automation" },
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

  const stepOneForm = stepOneContainer ? stepOneContainer.closest("form") : null;
  const templateUrl = root.getAttribute("data-template-url");
  const templateButton = root.querySelector("[data-template-fill]");
  const templateFeedback = root.querySelector("[data-template-feedback]");
  let templateCache = null;
  let templatePromise = null;

  function ensureAutoSelectedSet() {
    if (!(setupState.autoSelected instanceof Set)) {
      const values = Array.isArray(setupState.autoSelected) ? setupState.autoSelected : [];
      setupState.autoSelected = new Set(values.map((value) => String(value)));
    }
    return setupState.autoSelected;
  }

  function isAutoSelected(moduleKey) {
    if (!moduleKey) {
      return false;
    }
    return ensureAutoSelectedSet().has(moduleKey);
  }

  function markAutoSelected(moduleKey, shouldMark) {
    if (!moduleKey) {
      return;
    }
    const autoSet = ensureAutoSelectedSet();
    if (shouldMark) {
      autoSet.add(moduleKey);
    } else {
      autoSet.delete(moduleKey);
    }
  }

  function ensureExplicitSelectedSet() {
    if (!(setupState.explicitSelected instanceof Set)) {
      const values = Array.isArray(setupState.explicitSelected) ? setupState.explicitSelected : [];
      setupState.explicitSelected = new Set(values.map((value) => String(value)));
    }
    return setupState.explicitSelected;
  }

  function markExplicitSelected(moduleKey, shouldMark) {
    if (!moduleKey) {
      return;
    }
    const explicitSet = ensureExplicitSelectedSet();
    if (shouldMark) {
      explicitSet.add(moduleKey);
    } else {
      explicitSet.delete(moduleKey);
    }
  }

  function moduleMeta(moduleKey) {
    if (!moduleKey || !setupState.modules) {
      return null;
    }
    return setupState.modules[moduleKey] || null;
  }

  function isModuleHidden(moduleKey) {
    if (!moduleKey) {
      return false;
    }
    const card = root.querySelector(`[data-module-card][data-module-key="${moduleKey}"]`);
    if (!card) {
      return false;
    }
    const attributeHidden = card.getAttribute("data-module-hidden");
    if (attributeHidden === "true") {
      return true;
    }
    return card.classList.contains("d-none");
  }

  function getSelectedModuleKeys() {
    const order = Array.isArray(setupState.order) ? setupState.order : [];
    const selected = setupState.selected || {};
    const selectedKeys = Object.keys(selected).filter((key) => {
      if (!selected[key]) {
        return false;
      }
      if (isAutoSelected(key)) {
        const meta = moduleMeta(key);
        if (meta) {
          const optionCount = Object.keys(meta.options || {}).length;
          if (optionCount > 1) {
            return false;
          }
        }
      }
      return true;
    });
    if (!order.length) {
      return selectedKeys;
    }
    return order.filter((key) => selectedKeys.includes(key));
  }

  function dependencyValue(moduleKey) {
    const meta = moduleMeta(moduleKey);
    if (!meta || !meta.depends_on) {
      return null;
    }
    const value = setupState.selected?.[meta.depends_on];
    return value == null ? null : String(value);
  }

  function visibleOptions(moduleKey) {
    const meta = moduleMeta(moduleKey);
    if (!meta) {
      return [];
    }
    const optionKeys = Object.keys(meta.options || {});
    const dependsOn = meta.depends_on;
    const visibleWhen = meta.visible_when || {};
    if (!dependsOn || Object.keys(visibleWhen).length === 0) {
      return optionKeys;
    }
    const dependency = dependencyValue(moduleKey);
    let allowed = Array.isArray(visibleWhen[dependency]) ? visibleWhen[dependency] : null;
    if (!allowed && Array.isArray(visibleWhen.__default__)) {
      allowed = visibleWhen.__default__;
    }
    if (!allowed) {
      return [];
    }
    return allowed.filter((key) => key in (meta.options || {}));
  }

  function shouldHideModule(moduleKey) {
    const meta = moduleMeta(moduleKey);
    if (!meta) {
      return false;
    }
    const hideWhen = Array.isArray(meta.hide_when) ? meta.hide_when : [];
    if (!hideWhen.length || !meta.depends_on) {
      return false;
    }
    const dependency = dependencyValue(moduleKey);
    return dependency != null && hideWhen.includes(dependency);
  }

  function defaultSelection(moduleKey, allowedOptions) {
    const meta = moduleMeta(moduleKey);
    if (!meta) {
      return null;
    }
    const dependsOn = meta.depends_on;
    const defaultFor = meta.default_for || {};
    if (dependsOn) {
      const dependency = dependencyValue(moduleKey);
      if (dependency != null && typeof defaultFor[dependency] === "string") {
        return defaultFor[dependency];
      }
    }
    if (typeof meta.default_option === "string") {
      if (!allowedOptions || allowedOptions.includes(meta.default_option)) {
        return meta.default_option;
      }
    }
    if (Array.isArray(allowedOptions) && allowedOptions.length === 1) {
      return allowedOptions[0];
    }
    return null;
  }

  function moduleSelectionMissing(moduleKey) {
    const meta = moduleMeta(moduleKey);
    if (!meta) {
      return false;
    }
    const allowed = visibleOptions(moduleKey);
    const hidden = isModuleHidden(moduleKey) || shouldHideModule(moduleKey) || (meta.depends_on && allowed.length === 0);
    if (hidden) {
      return false;
    }
    if (!Array.isArray(allowed) || allowed.length <= 1) {
      return false;
    }
    const inputs = getModuleOptionInputs(moduleKey).filter((input) => !input.disabled);
    if (!inputs.length) {
      return false;
    }
    if (isAutoSelected(moduleKey) && !hidden && allowed.length > 1) {
      const selectedValue = setupState.selected?.[moduleKey];
      const optionMeta = meta.options?.[selectedValue] || null;
      const skipAuto = Boolean(optionMeta && optionMeta.skip_configuration);
      if (!skipAuto) {
        return true;
      }
    }
    return !inputs.some((input) => input.checked);
  }

  function toggleModuleVisibility(moduleKey, hidden) {
    const card = root.querySelector(`[data-module-card][data-module-key="${moduleKey}"]`);
    if (!card) {
      return;
    }
    card.setAttribute("data-module-hidden", hidden ? "true" : "false");
    if (hidden) {
      card.classList.add("d-none");
      card.setAttribute("hidden", "hidden");
      card.setAttribute("aria-hidden", "true");
    } else {
      card.classList.remove("d-none");
      card.removeAttribute("hidden");
      card.removeAttribute("aria-hidden");
    }
  }

  function updateModuleMissingIndicator(moduleKey) {
    const indicator = root.querySelector(`[data-module-missing="${moduleKey}"]`);
    if (!indicator) {
      return;
    }
    const missing = moduleSelectionMissing(moduleKey);
    indicator.classList.toggle("d-none", !missing);
  }

  function updateStepOneMissingBadges() {
    const groupButtons = Array.from(root.querySelectorAll('[data-step1-group]'));
    groupButtons.forEach((button) => {
      const groupKey = button.getAttribute("data-step1-group");
      const moduleKeys = getGroupModuleKeys(groupKey);
      let missingCount = 0;
      let visibleCount = 0;
      moduleKeys.forEach((moduleKey) => {
        const meta = moduleMeta(moduleKey);
        if (!meta) {
          return;
        }
        const allowed = visibleOptions(moduleKey);
        const hidden = isModuleHidden(moduleKey) || shouldHideModule(moduleKey) || (meta.depends_on && allowed.length === 0);
        if (!hidden) {
          visibleCount += 1;
        }
        if (moduleSelectionMissing(moduleKey)) {
          missingCount += 1;
        }
      });
      button.setAttribute("data-group-visible", String(visibleCount));
      button.setAttribute("data-group-missing", String(missingCount));
      const badge = button.querySelector('[data-step1-nav-missing]');
      if (badge) {
        badge.classList.toggle("d-none", missingCount === 0);
        if (missingCount > 0) {
          badge.textContent = `${missingCount} missing`;
        }
      }
      const label = button.querySelector("small.text-muted");
      if (label) {
        const total = Number(button.getAttribute("data-group-total") || 0) || moduleKeys.length;
        const plural = total === 1 ? "" : "s";
        label.textContent = `${visibleCount} active of ${total} module${plural}`;
      }
      const sectionActive = root.querySelector(`[data-step1-section-active="${groupKey}"]`);
      if (sectionActive) {
        const total = moduleKeys.length;
        const plural = total === 1 ? "" : "s";
        sectionActive.textContent = `${visibleCount} active of ${total} module${plural}`;
      }
      const sectionBadge = root.querySelector(`[data-step1-section-missing="${groupKey}"]`);
      if (sectionBadge) {
        sectionBadge.classList.toggle("d-none", missingCount === 0);
        if (missingCount > 0) {
          sectionBadge.textContent = `${missingCount} missing`;
        }
      }
    });
  }

  function pendingFieldsForModule(moduleKey) {
    const meta = moduleMeta(moduleKey);
    if (!meta) {
      return 0;
    }
    const selectedKey = setupState.selected?.[moduleKey];
    if (!selectedKey) {
      return 0;
    }
    const optionMeta = meta.options?.[selectedKey];
    if (!optionMeta) {
      return 0;
    }
    const fields = Array.isArray(optionMeta.fields) ? optionMeta.fields : [];
    const values = setupState.configuration?.[moduleKey] || {};
    let pending = 0;
    fields.forEach((field) => {
      if (!field || field.optional) {
        return;
      }
      const value = values[field.name];
      if (value == null || String(value).trim() === "") {
        pending += 1;
      }
    });
    return pending;
  }

  function updateConfigurationBadges() {
    const groupNavs = Array.from(root.querySelectorAll('[data-wizard-group]'));
    groupNavs.forEach((nav) => {
      const groupKey = nav.getAttribute("data-wizard-group");
      let groupPending = 0;
      const buttons = Array.from(nav.querySelectorAll('[data-module-target]'));
      buttons.forEach((button) => {
        const moduleKey = button.getAttribute("data-module-target");
        const pending = pendingFieldsForModule(moduleKey);
        groupPending += pending;
        button.setAttribute("data-module-pending", String(pending));
        const badge = button.querySelector('[data-module-nav-pending]');
        if (badge) {
          badge.classList.toggle("d-none", pending === 0);
          if (pending > 0) {
            badge.textContent = `${pending} missing`;
          }
        }
        const headerBadge = root.querySelector(`[data-module-header-pending="${moduleKey}"]`);
        if (headerBadge) {
          headerBadge.classList.toggle("d-none", pending === 0);
          if (pending > 0) {
            headerBadge.textContent = `${pending} required`;
          }
        }
      });
      nav.setAttribute("data-group-pending", String(groupPending));
      const groupBadge = root.querySelector(`[data-group-nav-pending="${groupKey}"]`);
      if (groupBadge) {
        groupBadge.classList.toggle("d-none", groupPending === 0);
        if (groupPending > 0) {
          groupBadge.textContent = `${groupPending} missing`;
        }
      }
    });
  }

  const initialSelectedKeys = getSelectedModuleKeys();
  let activeModuleKey = initialSelectedKeys[0] || (Array.isArray(setupState.order) ? setupState.order[0] : null);
  let activeGroupKey = null;
  let diagramCounter = 0;

  applyModuleDependencies();

  function ensureConfiguration(moduleKey) {
    if (!setupState.configuration[moduleKey] || typeof setupState.configuration[moduleKey] !== "object") {
      setupState.configuration[moduleKey] = {};
    }
    return setupState.configuration[moduleKey];
  }

  function clearTemplateFeedback() {
    if (!templateFeedback) {
      return;
    }
    templateFeedback.classList.add("d-none");
    templateFeedback.classList.remove("alert-success", "alert-warning", "alert-danger");
    if (!templateFeedback.classList.contains("alert-info")) {
      templateFeedback.classList.add("alert-info");
    }
    templateFeedback.textContent = "";
  }

  function showTemplateFeedback(kind, message) {
    if (!templateFeedback) {
      return;
    }
    const variants = ["info", "success", "warning", "danger"];
    const classNames = variants.map((variant) => `alert-${variant}`);
    templateFeedback.classList.remove(...classNames);
    const variant = variants.includes(kind) ? kind : "info";
    templateFeedback.classList.add(`alert-${variant}`);
    templateFeedback.classList.remove("d-none");
    templateFeedback.textContent = message;
  }

  async function loadTemplateData() {
    if (!templateUrl) {
      throw new Error("Sample configuration template URL is not available.");
    }
    if (templateCache) {
      return templateCache;
    }
    if (templatePromise) {
      return templatePromise;
    }
    templatePromise = fetch(templateUrl, { cache: "no-store" })
      .then((response) => {
        if (!response.ok) {
          throw new Error(`Template fetch failed with status ${response.status}`);
        }
        return response.json();
      })
      .then((data) => {
        templateCache = data;
        return data;
      })
      .finally(() => {
        templatePromise = null;
      });
    return templatePromise;
  }

  function applyTemplateValues(template) {
    const modulesTemplate = template && typeof template === "object" && template.modules ? template.modules : {};
    const result = {
      applied: 0,
      total: 0,
      missing: [],
    };
    if (!modulesTemplate || typeof modulesTemplate !== "object") {
      return result;
    }
    const selections = setupState.selected || {};
    Object.entries(selections).forEach(([moduleKey, optionKeyRaw]) => {
      const optionKey = String(optionKeyRaw);
      const moduleTemplate = modulesTemplate[moduleKey];
      if (!moduleTemplate || typeof moduleTemplate !== "object") {
        result.missing.push(moduleKey);
        return;
      }
      const optionTemplate = moduleTemplate[optionKey];
      if (!optionTemplate || typeof optionTemplate !== "object") {
        result.missing.push(`${moduleKey}:${optionKey}`);
        return;
      }
      const entries = Object.entries(optionTemplate);
      if (entries.length === 0) {
        markExplicitSelected(moduleKey, true);
      }
      entries.forEach(([fieldName, rawValue]) => {
        result.total += 1;
        const value = rawValue == null ? "" : String(rawValue);
        const selector = `[name="config__${moduleKey}__${fieldName}"]`;
        const field = root.querySelector(selector);
        if (
          field instanceof HTMLInputElement ||
          field instanceof HTMLTextAreaElement ||
          field instanceof HTMLSelectElement
        ) {
          if (field.value !== value) {
            field.value = value;
          }
          field.dispatchEvent(new Event("input", { bubbles: true }));
          result.applied += 1;
        } else {
          const configuration = ensureConfiguration(moduleKey);
          configuration[fieldName] = value;
        }
      });
    });
    return result;
  }

  function bindTemplateGenerator() {
    if (!templateButton) {
      return;
    }
    if (!templateUrl) {
      templateButton.disabled = true;
      templateButton.classList.add("disabled");
      return;
    }
    const idleLabel = templateButton.textContent || "";
    const busyLabel = templateButton.getAttribute("data-template-busy-text") || "Generating sample values…";
    templateButton.addEventListener("click", async () => {
      if (templateButton.hasAttribute("data-template-busy")) {
        return;
      }
      templateButton.setAttribute("data-template-busy", "1");
      templateButton.disabled = true;
      templateButton.setAttribute("aria-busy", "true");
      templateButton.textContent = busyLabel;
      clearTemplateFeedback();
      try {
        const template = await loadTemplateData();
        const { applied, total, missing } = applyTemplateValues(template || {});
        if (applied > 0) {
          let message = `Applied ${applied} sample value${applied === 1 ? "" : "s"}.`;
          if (total > applied || (Array.isArray(missing) && missing.length > 0)) {
            const missingText = missing.length ? ` Some modules were skipped: ${missing.join(", ")}.` : "";
            showTemplateFeedback("warning", `${message}${missingText}`);
          } else {
            showTemplateFeedback("success", message);
          }
        } else {
          showTemplateFeedback(
            "warning",
            "No matching fields were found for the sample template. Check your selections and try again."
          );
        }
      } catch (error) {
        console.error("Unable to apply setup wizard sample configuration", error);
        showTemplateFeedback(
          "danger",
          "Loading the sample configuration failed. Refresh the page or update the static template."
        );
      } finally {
        templateButton.textContent = idleLabel;
        templateButton.removeAttribute("aria-busy");
        templateButton.removeAttribute("data-template-busy");
        templateButton.disabled = false;
      }
    });
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
    const selectedKeys = getSelectedModuleKeys();
    const explicitSet = ensureExplicitSelectedSet();
    const hasExplicitSelection = selectedKeys.some((moduleKey) => explicitSet.has(moduleKey));
    if (!hasExplicitSelection) {
      return null;
    }

    const hasMeaningfulSelection = selectedKeys.some((moduleKey) => {
      const meta = moduleMeta(moduleKey);
      if (!meta) {
        return false;
      }
      const optionKeys = Object.keys(meta.options || {});
      if (optionKeys.length <= 1) {
        return false;
      }
      if (isModuleHidden(moduleKey)) {
        return false;
      }
      if (isAutoSelected(moduleKey)) {
        return false;
      }
      const value = setupState.selected?.[moduleKey];
      return value != null && value !== "";
    });

    if (!hasMeaningfulSelection) {
      return null;
    }

    const includeDetails = step >= 2;
    const nodes = {
      auth: buildNodeLabel("authentication", includeDetails),
      ui: buildNodeLabel("user_interface", includeDetails),
      ui_dep: buildNodeLabel("ui_deployment", includeDetails),
      gov: buildNodeLabel("governance_service", includeDetails),
      gov_dep: buildNodeLabel("governance_deployment", includeDetails),
      ext: buildNodeLabel("governance_extensions", includeDetails),
      pipeline: buildNodeLabel("pipeline_integration", includeDetails),
      contracts: buildNodeLabel("contracts_backend", includeDetails),
      products: buildNodeLabel("products_backend", includeDetails),
      gov_store: buildNodeLabel("governance_store", includeDetails),
      dq: buildNodeLabel("data_quality", includeDetails),
      demo: buildNodeLabel("demo_automation", includeDetails),
    };

    const hasModule = (moduleKey, stack) => {
      if (!moduleKey) {
        return false;
      }
      const moduleMeta = setupState.modules?.[moduleKey];
      if (!moduleMeta || isModuleHidden(moduleKey)) {
        return false;
      }

      const recursionStack = stack instanceof Set ? stack : new Set();
      if (recursionStack.has(moduleKey)) {
        return false;
      }
      recursionStack.add(moduleKey);

      const explicitSet = ensureExplicitSelectedSet();
      const dependencyKey = moduleMeta.depends_on;

      if (!explicitSet.has(moduleKey) && !dependencyKey) {
        recursionStack.delete(moduleKey);
        return false;
      }

      const selectedValue = setupState.selected?.[moduleKey];
      if (selectedValue == null || selectedValue === "") {
        recursionStack.delete(moduleKey);
        return false;
      }

      const optionKeys = Object.keys(moduleMeta.options || {});
      const auto = isAutoSelected(moduleKey);

      if (dependencyKey) {
        const dependencyActive = hasModule(dependencyKey, recursionStack);
        if (!dependencyActive) {
          recursionStack.delete(moduleKey);
          return false;
        }
        if (optionKeys.length <= 1) {
          const includeSingleOption =
            explicitSet.has(moduleKey) || explicitSet.has(dependencyKey);
          recursionStack.delete(moduleKey);
          return includeSingleOption;
        }
        if (auto && !explicitSet.has(moduleKey) && !explicitSet.has(dependencyKey)) {
          recursionStack.delete(moduleKey);
          return false;
        }
        recursionStack.delete(moduleKey);
        return true;
      }

      if (optionKeys.length <= 1) {
        const includeSingleOption = explicitSet.has(moduleKey);
        recursionStack.delete(moduleKey);
        return includeSingleOption;
      }

      if (auto && !explicitSet.has(moduleKey)) {
        recursionStack.delete(moduleKey);
        return false;
      }

      recursionStack.delete(moduleKey);
      return true;
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
      "  classDef automation fill:#f3e5f5,stroke:#8e44ad,color:#5f249f;",
      "  classDef external fill:#fff8e1,stroke:#f0ad4e,color:#a15c0f;",
    ];

    const definedNodes = new Set();
    const externalNodeMap = new Map();
    const externalEdges = [];

    function resolveNodeRef(ref) {
      if (!ref) {
        return null;
      }
      if (moduleNodeMap[ref]) {
        return moduleNodeMap[ref].id;
      }
      return String(ref);
    }

    function defineNode(moduleKey, indent = "    ") {
      const nodeMeta = moduleNodeMap[moduleKey];
      if (!nodeMeta || !hasModule(moduleKey)) {
        return;
      }
      const label = nodes[nodeMeta.id];
      lines.push(`${indent}${nodeMeta.id}["${label}"]`);
      definedNodes.add(moduleKey);
    }

    function registerExternalNode(moduleKey, rawNode) {
      if (!rawNode || !rawNode.id) {
        return;
      }
      const nodeId = String(rawNode.id);
      if (!externalNodeMap.has(nodeId)) {
        externalNodeMap.set(nodeId, {
          id: nodeId,
          label: sanitizeLabel(rawNode.label || nodeId),
          className:
            rawNode.className || rawNode.class || rawNode.class_name || "external",
        });
      }

      const edges = Array.isArray(rawNode.edges) && rawNode.edges.length
        ? rawNode.edges
        : [
            {
              from: moduleKey,
              to: nodeId,
              label: rawNode.edgeLabel || null,
            },
          ];

      for (const edge of edges) {
        const fromId = resolveNodeRef(edge.from || moduleKey);
        const toId = resolveNodeRef(edge.to || nodeId);
        if (!fromId || !toId) {
          continue;
        }
        externalEdges.push({
          from: fromId,
          to: toId,
          label: edge.label ? sanitizeLabel(edge.label) : null,
        });
      }
    }

    const pushSubgraph = (title, moduleKeys, indent = "  ") => {
      const list = Array.isArray(moduleKeys) ? moduleKeys : [];
      const active = list.filter((key) => hasModule(key));
      if (!active.length) {
        return;
      }
      const safeTitle = sanitizeLabel(title || "Selection");
      lines.push(`${indent}subgraph "${safeTitle}"`);
      lines.push(`${indent}  direction TB`);
      for (const moduleKey of active) {
        defineNode(moduleKey, `${indent}  `);
      }
      lines.push(`${indent}end`);
    };

    const pushGroupedSubgraphs = (groupTitle, groups) => {
      const entries = Array.isArray(groups) ? groups : [];
      const activeGroups = entries.filter((group) =>
        group && Array.isArray(group.modules) && group.modules.some((moduleKey) => hasModule(moduleKey))
      );
      if (!activeGroups.length) {
        return;
      }
      lines.push(`  subgraph "${sanitizeLabel(groupTitle || "dc43 modules")}"`);
      lines.push("    direction TB");
      for (const group of activeGroups) {
        pushSubgraph(group.title, group.modules, "    ");
      }
      lines.push("  end");
    };

    pushGroupedSubgraphs("Pipeline footprint", [
      { title: "Pipeline integration", modules: ["pipeline_integration"] },
      {
        title: "Orchestration & quality",
        modules: ["governance_service", "data_quality", "governance_extensions"],
      },
      {
        title: "Persistent storage",
        modules: ["contracts_backend", "products_backend", "governance_store"],
      },
    ]);

    pushGroupedSubgraphs("Operator experience", [
      { title: "Interface & access", modules: ["user_interface", "authentication"] },
      { title: "Accelerators", modules: ["demo_automation"] },
    ]);

    const hostingGroups = [];
    if (hasModule("ui_deployment")) {
      hostingGroups.push({ title: "UI hosting", modules: ["ui_deployment"] });
    }
    if (hasModule("governance_deployment")) {
      const choice = setupState.selected?.governance_deployment;
      const localOptions = new Set(["local_python", "local_docker", "not_required"]);
      const title = localOptions.has(choice) ? "Local runtime" : "Hosted deployments";
      hostingGroups.push({ title, modules: ["governance_deployment"] });
    }
    if (hostingGroups.length) {
      pushGroupedSubgraphs("Operations & hosting", hostingGroups);
    }

    if (setupState.modules && typeof setupState.modules === "object") {
      for (const [moduleKey, moduleMeta] of Object.entries(setupState.modules)) {
        if (!moduleMeta || !hasModule(moduleKey)) {
          continue;
        }
        const optionMeta = moduleOptionMeta(moduleKey);
        const diagram = optionMeta?.option?.diagram;
        if (!diagram || !Array.isArray(diagram.nodes)) {
          continue;
        }
        for (const rawNode of diagram.nodes) {
          registerExternalNode(moduleKey, rawNode);
        }
      }
    }

    if (hasModule("user_interface") && hasModule("governance_service")) {
      lines.push("  ui -->|Orchestrates| gov");
    }
    if (hasModule("authentication") && hasModule("user_interface")) {
      lines.push("  auth -->|Protects| ui");
    }
    if (hasModule("pipeline_integration") && hasModule("governance_service")) {
      lines.push("  pipeline -->|Invokes| gov");
    }
    if (hasModule("pipeline_integration") && hasModule("data_quality")) {
      lines.push("  pipeline -->|Requests checks| dq");
    }
    if (hasModule("pipeline_integration") && hasModule("contracts_backend")) {
      lines.push("  pipeline -->|Reads contracts| contracts");
    }
    if (hasModule("pipeline_integration") && hasModule("products_backend")) {
      lines.push("  pipeline -->|Publishes releases| products");
    }
    if (hasModule("pipeline_integration") && hasModule("governance_store")) {
      lines.push("  pipeline -->|Records outcomes| gov_store");
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
    if (hasModule("governance_service") && hasModule("governance_store")) {
      lines.push("  gov -->|Persists results| gov_store");
    }
    if (hasModule("data_quality") && hasModule("governance_store")) {
      lines.push("  dq -->|Writes outcomes| gov_store");
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
    if (hasModule("demo_automation") && hasModule("governance_service")) {
      lines.push("  demo -->|Bootstraps| gov");
    }
    if (hasModule("demo_automation") && hasModule("user_interface")) {
      lines.push("  demo -->|Opens| ui");
    }

    const externalNodes = Array.from(externalNodeMap.values());
    if (externalNodes.length) {
      lines.push(`  subgraph "${sanitizeLabel("External platforms & integrations")}"`);
      lines.push("    direction TB");
      for (const node of externalNodes) {
        lines.push(`    ${node.id}["${node.label}"]`);
      }
      lines.push("  end");
    }

    for (const edge of externalEdges) {
      const label = edge.label ? `|${edge.label}|` : "";
      lines.push(`  ${edge.from} -->${label} ${edge.to}`);
    }

    for (const [moduleKey, nodeMeta] of Object.entries(moduleNodeMap)) {
      if (!definedNodes.has(moduleKey) || !nodeMeta.className) {
        continue;
      }
      lines.push(`  class ${nodeMeta.id} ${nodeMeta.className};`);
    }

    for (const node of externalNodeMap.values()) {
      if (!node.className) {
        continue;
      }
      lines.push(`  class ${node.id} ${node.className};`);
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
    if (!definition) {
      mermaidContainer.innerHTML = '<div class="text-muted small text-center">Select modules to build your architecture overview.</div>';
      return;
    }
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
      highlightKey = moduleKeys.find((key) => setupState.selected[key] && !isAutoSelected(key) && !isModuleHidden(key));
    }
    if (!highlightKey) {
      highlightKey = moduleKeys.find((key) => !isAutoSelected(key) && !isModuleHidden(key)) || null;
    }
    if (!highlightKey) {
      highlightKey = moduleKeys.find((key) => !isModuleHidden(key)) || moduleKeys[0] || null;
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

  function getModuleOptionInputs(moduleKey) {
    if (!moduleKey) {
      return [];
    }
    return Array.from(
      root.querySelectorAll(`input[type="radio"][name="module__${moduleKey}"]`),
    );
  }

  function applyModuleDependencies() {
    if (!setupState.modules) {
      return;
    }
    const moduleKeys = Array.isArray(setupState.order)
      ? setupState.order
      : Object.keys(setupState.modules);
    moduleKeys.forEach((moduleKey) => {
      const meta = moduleMeta(moduleKey);
      if (!meta) {
        return;
      }
      const allowed = visibleOptions(moduleKey);
      const hide = shouldHideModule(moduleKey) || (meta.depends_on && allowed.length === 0);
      toggleModuleVisibility(moduleKey, hide);
      const inputs = getModuleOptionInputs(moduleKey);
      inputs.forEach((input) => {
        const isAllowed = !allowed.length || allowed.includes(input.value);
        const disable = hide || !isAllowed;
        input.disabled = disable;
        if (disable && input.checked) {
          input.checked = false;
        }
      });

      let selection = setupState.selected?.[moduleKey] || null;
      let selectionIsAuto = isAutoSelected(moduleKey);
      if (hide) {
        const fallback = defaultSelection(moduleKey, allowed);
        if (fallback) {
          selection = fallback;
          selectionIsAuto = true;
        }
      } else {
        const checked = inputs.find((input) => input.checked && !input.disabled);
        if (checked) {
          selection = checked.value;
          selectionIsAuto = false;
        }
        if (selection && allowed.length && !allowed.includes(selection)) {
          selection = null;
        }
        if (!selection) {
          const fallback = defaultSelection(moduleKey, allowed);
          if (fallback) {
            selection = fallback;
            selectionIsAuto = true;
          }
        }
      }

      if (selection) {
        setupState.selected[moduleKey] = selection;
        markAutoSelected(moduleKey, selectionIsAuto && allowed.length > 1);
        if (selectionIsAuto) {
          markExplicitSelected(moduleKey, false);
        }
        inputs.forEach((input) => {
          if (!input.disabled) {
            input.checked = input.value === selection;
          } else if (hide && input.value === selection) {
            input.checked = true;
          }
        });
      } else {
        delete setupState.selected[moduleKey];
        markAutoSelected(moduleKey, false);
        markExplicitSelected(moduleKey, false);
        inputs.forEach((input) => {
          if (!input.disabled) {
            input.checked = false;
          }
        });
      }

      updateModuleMissingIndicator(moduleKey);
    });

    updateStepOneMissingBadges();
    updateConfigurationBadges();
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
        markAutoSelected(moduleKey, false);
        markExplicitSelected(moduleKey, true);
        applyModuleDependencies();
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
      const button = stepOneContainer?.querySelector?.(`[data-step1-nav="${groupKey}"]`);
      if (!button) {
        return false;
      }
      const missing = Number(button.getAttribute("data-group-missing") || 0);
      return Number.isFinite(missing) && missing > 0;
    });

    if (!initialGroupKey) {
      initialGroupKey = groupKeys.find((groupKey) => {
        const moduleKeys = getGroupModuleKeys(groupKey);
        return moduleKeys.some((moduleKey) => setupState.selected[moduleKey] && !isAutoSelected(moduleKey));
      });
    }

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
        } else if (stepOneForm instanceof HTMLFormElement) {
          const blockingGroup = groupKeys.find((key) => {
            const button = stepOneContainer.querySelector(`[data-step1-nav="${key}"]`);
            if (!button) {
              return false;
            }
            const missing = Number(button.getAttribute("data-group-missing") || 0);
            return Number.isFinite(missing) && missing > 0;
          });
          if (blockingGroup) {
            setActiveGroup(blockingGroup, { scrollIntoView: true });
            return;
          }
          if (typeof stepOneForm.requestSubmit === "function") {
            stepOneForm.requestSubmit();
          } else {
            stepOneForm.submit();
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
        markExplicitSelected(moduleKey, true);
        renderDiagram(activeModuleKey);
        updateConfigurationBadges();
      });
    });
    updateConfigurationBadges();
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
  bindTemplateGenerator();
  bindWizardNav();
  updateWizardVisibility();
  updateWizardNav();
  renderDiagram(activeModuleKey);
}
