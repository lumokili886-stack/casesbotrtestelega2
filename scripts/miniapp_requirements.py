from pathlib import Path
import os
from playwright.sync_api import sync_playwright

URL = os.getenv("MINIAPP_URL", "http://127.0.0.1:4173/")
OUT = Path('/Users/maksimgubarev/Documents/Codex/2026-04-23-files-mentioned-by-the-user-cs2-2/vault/artifacts/miniapp-requirements')
OUT.mkdir(parents=True, exist_ok=True)

checks = []
errors = []


def parse_balance(text: str) -> int:
    digits = ''.join(ch for ch in text if ch.isdigit())
    return int(digits) if digits else 0


with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 430, "height": 932})

    page.on("pageerror", lambda e: errors.append(f"pageerror: {e}"))
    page.on("console", lambda m: errors.append(f"console[{m.type}]: {m.text}") if m.type == "error" else None)

    page.goto(URL, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(1200)
    checks.append(("Home loaded", page.locator("#screen-home.active").count() == 1))

    # 1) Skin images are visible and loaded
    image_count = page.locator("img[src^='/assets/skins/']").count()
    checks.append(("Skin images rendered on page", image_count >= 5))
    first_ok = page.evaluate(
        "() => { const img = document.querySelector(\"img[src^='/assets/skins/']\"); return !!img && img.complete && img.naturalWidth > 0; }"
    )
    checks.append(("At least one skin image loaded", bool(first_ok)))
    page.screenshot(path=str(OUT / "01-images-home.png"), full_page=True)

    # open a case and keep item to create history entry
    page.locator("#screen-home .case-card").first.click()
    page.wait_for_timeout(350)
    page.locator("#open-btn").click()
    page.wait_for_timeout(6200)
    checks.append(("Win overlay visible", page.locator("#win-overlay.show").count() == 1))
    page.locator(".win-keep").click()
    page.wait_for_timeout(250)
    page.locator("#screen-open .back-btn").click()
    page.wait_for_timeout(300)

    # 2) History of won items opens on separate screen
    page.locator("#screen-home .bottom-nav .nav-item").nth(3).click()
    page.wait_for_timeout(400)
    checks.append(("Profile screen active", page.locator("#screen-profile.active").count() == 1))
    page.locator("#screen-profile .menu-item").first.click()
    page.wait_for_timeout(350)
    checks.append(("History screen active", page.locator("#screen-history.active").count() == 1))
    history_count = page.locator("#history-list .history-item").count()
    checks.append(("History list rendered", history_count >= 1))
    first_history_text = page.locator("#history-list .history-item").first.inner_text()
    checks.append(("History has meaningful content", "История пока пустая" not in first_history_text))
    page.screenshot(path=str(OUT / "02-history-screen.png"), full_page=True)

    # back to profile
    page.locator("#screen-history .back-btn").click()
    page.wait_for_timeout(300)
    checks.append(("Back from history returns to profile", page.locator("#screen-profile.active").count() == 1))

    # 3) Only Stars method on deposit screen
    page.locator("#screen-profile .menu-item").nth(3).click()
    page.wait_for_timeout(350)
    checks.append(("Deposit screen active", page.locator("#screen-deposit.active").count() == 1))
    checks.append(("No crypto/card methods in deposit", page.locator("#screen-deposit .method-pill").count() == 0))
    deposit_html = page.locator("#screen-deposit").inner_text()
    checks.append(("No 'Крипто' in deposit text", "Крипто" not in deposit_html))
    checks.append(("No 'Карта' in deposit text", "Карта" not in deposit_html))

    # 4) Manual stars amount input with min/max limits
    bal_before = parse_balance(page.locator("#balance2").inner_text()) if page.locator("#balance2").count() else 0

    page.locator("#deposit-input").fill("0")
    page.wait_for_timeout(150)
    page.locator("#deposit-btn").click()
    page.wait_for_timeout(250)
    toast_0 = page.locator("#toast").inner_text()
    checks.append(("Validation blocks amount 0", "от 1 до 5000" in toast_0))

    page.locator("#deposit-input").fill("5001")
    page.wait_for_timeout(150)
    page.locator("#deposit-btn").click()
    page.wait_for_timeout(250)
    toast_5001 = page.locator("#toast").inner_text()
    checks.append(("Validation blocks amount >5000", "от 1 до 5000" in toast_5001))

    page.locator("#deposit-input").fill("1234")
    page.wait_for_timeout(150)
    page.locator("#deposit-btn").click()
    page.wait_for_timeout(500)

    # After demo top-up we navigate back; detect active screen and read balance there
    active_screen = page.evaluate("document.querySelector('.screen.active')?.id || ''")
    if active_screen == "screen-home":
        bal_after = parse_balance(page.locator("#balance").inner_text())
    elif active_screen == "screen-inventory":
        bal_after = parse_balance(page.locator("#balance2").inner_text())
    elif active_screen == "screen-profile":
        bal_after = parse_balance(page.locator("#balance3").inner_text())
    else:
        bal_after = parse_balance(page.locator("#balance").inner_text())
    checks.append(("Valid amount increases balance", bal_after > bal_before))
    page.screenshot(path=str(OUT / "03-deposit-validation.png"), full_page=True)

    browser.close()

report = ["Miniapp requirements regression", f"URL: {URL}", "", "Checks:"]
for name, ok in checks:
    report.append(f"- {'OK' if ok else 'FAIL'}: {name}")

if errors:
    report.append("")
    report.append("Console/Page errors:")
    for e in errors:
        report.append(f"- {e}")

(OUT / "report.txt").write_text("\n".join(report), encoding="utf-8")
print("\n".join(report))
print(f"\nArtifacts: {OUT}")
