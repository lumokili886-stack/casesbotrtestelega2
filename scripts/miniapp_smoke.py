from pathlib import Path
import os
from playwright.sync_api import sync_playwright

URL = os.getenv("MINIAPP_URL", "https://lumokili886-stack.github.io/casesbotrtestelega2/")
OUT = Path('/Users/maksimgubarev/Documents/Codex/2026-04-23-files-mentioned-by-the-user-cs2-2/vault/artifacts/miniapp-smoke')
OUT.mkdir(parents=True, exist_ok=True)

results = []
errors = []

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 430, "height": 932})

    page.on("pageerror", lambda e: errors.append(f"pageerror: {e}"))
    page.on("console", lambda m: errors.append(f"console[{m.type}]: {m.text}") if m.type == "error" else None)

    page.goto(URL, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(1200)

    results.append(("URL", page.url))
    results.append(("Has VAULT logo", page.locator(".logo", has_text="VAULT").count() > 0))
    page.screenshot(path=str(OUT / "01-home.png"), full_page=True)

    # nav -> inventory
    page.locator(".bottom-nav .nav-item").nth(2).click()
    page.wait_for_timeout(400)
    results.append(("Inventory screen", page.locator("#screen-inventory.active").count() == 1))
    page.screenshot(path=str(OUT / "02-inventory.png"), full_page=True)

    # nav -> profile
    page.locator("#screen-inventory .bottom-nav .nav-item").nth(3).click()
    page.wait_for_timeout(400)
    results.append(("Profile screen", page.locator("#screen-profile.active").count() == 1))
    page.screenshot(path=str(OUT / "03-profile.png"), full_page=True)

    # open deposit from profile menu
    page.locator("#screen-profile .menu-item", has_text="Пополнить баланс").click()
    page.wait_for_timeout(400)
    results.append(("Deposit screen", page.locator("#screen-deposit.active").count() == 1))
    page.screenshot(path=str(OUT / "04-deposit.png"), full_page=True)

    # back
    page.locator("#deposit-back").click()
    page.wait_for_timeout(400)

    # go home and open first case
    page.locator("#screen-profile .bottom-nav .nav-item").first.click()
    page.wait_for_timeout(400)
    page.locator("#screen-home .case-card").first.click()
    page.wait_for_timeout(400)
    results.append(("Case open screen", page.locator("#screen-open.active").count() == 1))
    page.screenshot(path=str(OUT / "05-open-case.png"), full_page=True)

    # try free open path
    page.locator("#screen-open .back-btn").click()
    page.wait_for_timeout(300)
    page.locator("#screen-home .hero-btn").click()
    page.wait_for_timeout(400)
    page.locator("#open-btn").click()
    page.wait_for_timeout(6200)
    results.append(("Win overlay visible after free open", page.locator("#win-overlay.show").count() == 1))
    page.screenshot(path=str(OUT / "06-win-overlay.png"), full_page=True)

    browser.close()

report_lines = ["Miniapp smoke test", f"URL: {URL}", "", "Checks:"]
for name, ok in results:
    report_lines.append(f"- {'OK' if ok else 'FAIL'}: {name}")

if errors:
    report_lines.append("")
    report_lines.append("Errors:")
    for e in errors:
        report_lines.append(f"- {e}")

(OUT / "report.txt").write_text("\n".join(report_lines), encoding="utf-8")
print("\n".join(report_lines))
print(f"\nArtifacts: {OUT}")
