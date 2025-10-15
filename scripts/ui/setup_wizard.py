"""Playwright runner for the contracts app setup wizard.

This script provides a thin wrapper around Playwright's sync API so that
teams can replay curated wizard scenarios locally or in CI without
hand-editing generated code. Scenarios are stored as JSON alongside this
module and can be extended with additional module selections or
configuration overrides.
"""
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, Mapping, Optional

from playwright.sync_api import Browser, Locator, Page, expect, sync_playwright


SCENARIO_FILE = Path(__file__).with_name("setup_wizard_scenarios.json")


@dataclass(frozen=True)
class WizardScenario:
    """Represents a deterministic wizard flow."""

    name: str
    description: str
    module_selections: Mapping[str, str]
    configuration_overrides: Mapping[str, str]

    @classmethod
    def from_mapping(cls, name: str, payload: Mapping[str, object]) -> "WizardScenario":
        """Validate and convert JSON payloads into scenario instances."""

        description = str(payload.get("description", ""))
        try:
            module_selections = dict(payload["moduleSelections"])
        except KeyError as exc:  # pragma: no cover - defensive programming
            raise ValueError(f"Scenario '{name}' is missing moduleSelections") from exc
        configuration_overrides = dict(payload.get("configurationOverrides", {}))
        return cls(
            name=name,
            description=description,
            module_selections=module_selections,
            configuration_overrides=configuration_overrides,
        )


@dataclass
class ActionRecord:
    """Describes a single action taken while exercising the wizard."""

    step: str
    action: str
    details: Mapping[str, str] = field(default_factory=dict)

    def serialise(self) -> Dict[str, object]:
        return {
            "step": self.step,
            "action": self.action,
            "details": {key: str(value) for key, value in self.details.items()},
        }


class ActionRecorder:
    """Collects wizard actions for later inspection."""

    def __init__(self) -> None:
        self._records: list[ActionRecord] = []

    @property
    def records(self) -> Iterable[ActionRecord]:
        return tuple(self._records)

    def record(self, step: str, action: str, **details: str) -> None:
        self._records.append(ActionRecord(step=step, action=action, details=details))

    def dump(self, destination: Path) -> None:
        payload = [record.serialise() for record in self._records]
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def print_summary(self) -> None:
        if not self._records:
            return
        print("\nWizard action history:")
        for record in self._records:
            fragments = [f"step={record.step}", f"action={record.action}"]
            if record.details:
                fragments.append(
                    "details="
                    + ", ".join(f"{key}={value}" for key, value in record.details.items())
                )
            print(" - " + "; ".join(fragments))


def load_scenarios(path: Path) -> Dict[str, WizardScenario]:
    """Load scenarios from ``path`` and return them keyed by name."""

    if not path.exists():  # pragma: no cover - runtime guard for users
        raise FileNotFoundError(
            f"Scenario file '{path}' does not exist. Pass --scenario-file to point to a custom location."
        )

    with path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)

    scenarios = {}
    for name, payload in raw.items():
        scenarios[name] = WizardScenario.from_mapping(name, payload)
    return scenarios


def list_scenarios(scenarios: Iterable[WizardScenario]) -> str:
    """Format the available scenarios for CLI output."""

    rows = [f"{scenario.name}: {scenario.description}".rstrip() for scenario in scenarios]
    return "\n".join(sorted(rows))


def _module_card(page: Page, module_key: str) -> Locator:
    """Return the first module card locator for ``module_key``."""

    locator = page.locator(
        f"[data-module-card][data-module-key=\"{module_key}\"]"
    ).first
    locator.wait_for(state="attached")
    return locator


def _ensure_module_visible(page: Page, module_key: str) -> Locator:
    """Ensure the module card for ``module_key`` is visible and return its locator."""

    card = _module_card(page, module_key)
    if card.is_visible():
        card.scroll_into_view_if_needed()
        return card

    group_key = card.evaluate(
        "el => el.closest('[data-step1-section]')?.getAttribute('data-step1-section')"
    )
    if group_key:
        nav_button = page.locator(f"[data-step1-nav=\"{group_key}\"]").first
        if nav_button.count() == 0:
            raise AssertionError(
                f"Could not find navigation button for module group '{group_key}' while selecting '{module_key}'."
            )
        nav_button.click()
        expect(card).to_be_visible()
        card.scroll_into_view_if_needed()
        return card

    # If there's no enclosing step section we still attempt to reveal the card.
    expect(card).to_be_visible()
    card.scroll_into_view_if_needed()
    return card


def fill_step_one(page: Page, scenario: WizardScenario, recorder: ActionRecorder) -> None:
    """Select modules according to ``scenario``."""

    for module_key, option_key in scenario.module_selections.items():
        card = _ensure_module_visible(page, module_key)
        option_locator = card.locator(
            f"input[type=\"radio\"][value=\"{option_key}\"]"
        ).first

        if option_locator.count() == 0:
            available = card.locator("input[type=\"radio\"]").evaluate_all(
                "els => els.map(el => el.value)"
            )
            raise AssertionError(
                "Module '{module}' has no option '{option}'. Available options: {available}."
                .format(module=module_key, option=option_key, available=", ".join(available))
            )

        if option_locator.is_disabled():
            raise AssertionError(
                f"Option '{option_key}' for module '{module_key}' is disabled."
            )

        expect(option_locator).to_be_visible()
        option_locator.check()
        recorder.record(
            "modules",
            "select_option",
            module=module_key,
            option=option_key,
        )
    page.get_by_role("button", name="Continue").click()
    recorder.record("modules", "continue")


def fill_step_two(page: Page, scenario: WizardScenario, recorder: ActionRecorder) -> None:
    """Fill configuration fields for the selected modules."""

    for field_name, value in scenario.configuration_overrides.items():
        locator = page.locator(f"[name=\"{field_name}\"]")
        expect(locator.first).to_be_visible()
        locator.first.fill(value)
        recorder.record(
            "configuration",
            "fill_field",
            field=field_name,
            value=value,
        )
    page.get_by_role("button", name="Review summary").click()
    recorder.record("configuration", "review_summary")


def complete_summary(page: Page, recorder: ActionRecorder) -> None:
    """Confirm the summary and finish the wizard."""

    expect(page.locator("h2", has_text="Summary")).to_be_visible()
    page.get_by_role("button", name="Mark setup as complete").click()
    recorder.record("summary", "confirm_completion")


def run_wizard_flow(
    browser: Browser,
    base_url: str,
    scenario: WizardScenario,
    *,
    keep_open: bool = False,
    screenshot_path: Optional[Path] = None,
    recorder: Optional[ActionRecorder] = None,
    step_through: bool = False,
) -> None:
    """Execute ``scenario`` against ``base_url`` using ``browser``."""

    recorder = recorder or ActionRecorder()

    page = browser.new_page()
    page.goto(f"{base_url.rstrip('/')}/setup?restart=1")

    fill_step_one(page, scenario, recorder)
    if step_through:
        input("Step 1 complete – press Enter to continue to Step 2...")

    fill_step_two(page, scenario, recorder)
    if step_through:
        input("Step 2 complete – press Enter to continue to the summary...")

    complete_summary(page, recorder)
    expect(page).to_have_url(f"{base_url.rstrip('/')}/")

    if screenshot_path:
        screenshot_path.parent.mkdir(parents=True, exist_ok=True)
        page.screenshot(path=str(screenshot_path))

    recorder.print_summary()

    if keep_open:
        print("Leaving browser open for inspection. Close the window to finish...")
        page.wait_for_timeout(3_600_000)  # 1 hour timeout
    else:
        page.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-url",
        default="http://localhost:8000",
        help="Root URL of the contracts app (default: %(default)s)",
    )
    parser.add_argument(
        "--scenario",
        default="happy_path",
        help="Scenario name to execute (use --list to see available options)",
    )
    parser.add_argument(
        "--scenario-file",
        type=Path,
        default=SCENARIO_FILE,
        help="Path to the scenario JSON file",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run the browser in headless mode",
    )
    parser.add_argument(
        "--keep-open",
        action="store_true",
        help="Do not close the browser at the end of the run (implies headed mode)",
    )
    parser.add_argument(
        "--screenshot",
        type=Path,
        help="Capture a screenshot at the end of the flow",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available scenarios and exit",
    )
    parser.add_argument(
        "--log-actions",
        type=Path,
        help="Persist the executed steps to a JSON file for later review",
    )
    parser.add_argument(
        "--step-through",
        action="store_true",
        help="Pause between wizard stages so you can review the UI before continuing",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    scenarios = load_scenarios(args.scenario_file)
    if args.list:
        print(list_scenarios(scenarios.values()))
        return

    if args.scenario not in scenarios:
        raise SystemExit(
            f"Unknown scenario '{args.scenario}'. Use --list to inspect available scenarios or update {args.scenario_file}."
        )

    scenario = scenarios[args.scenario]

    if args.keep_open and args.headless:
        raise SystemExit("--keep-open cannot be combined with --headless")

    if args.step_through and args.headless:
        raise SystemExit("--step-through requires headed mode")

    keep_open = args.keep_open or args.step_through
    headless = args.headless or not keep_open

    recorder = ActionRecorder()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        try:
            run_wizard_flow(
                browser,
                args.base_url,
                scenario,
                keep_open=keep_open,
                screenshot_path=args.screenshot,
                recorder=recorder,
                step_through=args.step_through,
            )
        finally:
            if not keep_open:
                browser.close()

    if args.log_actions:
        recorder.dump(args.log_actions)
        print(f"Saved action history to {args.log_actions}")


if __name__ == "__main__":  # pragma: no cover - script entry point
    main()
