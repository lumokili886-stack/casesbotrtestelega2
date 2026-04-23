from pathlib import Path
from playwright.sync_api import sync_playwright

URL = 'http://127.0.0.1:4173/'
OUT = Path('/Users/maksimgubarev/Documents/Codex/2026-04-23-files-mentioned-by-the-user-cs2-2/vault/artifacts/miniapp-regression')
OUT.mkdir(parents=True, exist_ok=True)

checks = []
errors = []


def parse_balance(text: str) -> int:
    digits = ''.join(ch for ch in text if ch.isdigit())
    return int(digits) if digits else 0

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 430, "height": 932})

    page.on('pageerror', lambda e: errors.append(f'pageerror: {e}'))
    page.on('console', lambda m: errors.append(f'console[{m.type}]: {m.text}') if m.type == 'error' else None)

    page.goto(URL, wait_until='domcontentloaded', timeout=60000)
    page.wait_for_timeout(1000)

    checks.append(('Home loaded', page.locator('#screen-home.active').count() == 1))
    start_balance = parse_balance(page.locator('#balance').inner_text())
    checks.append(('Start balance visible', start_balance > 0))

    # Open paid case in demo mode
    page.locator('#screen-home .case-card').first.click()
    page.wait_for_timeout(300)
    checks.append(('Open screen active', page.locator('#screen-open.active').count() == 1))
    page.locator('#open-btn').click()
    page.wait_for_timeout(6200)
    checks.append(('Win overlay after paid open', page.locator('#win-overlay.show').count() == 1))
    page.screenshot(path=str(OUT / '01-paid-open-win.png'), full_page=True)

    # Keep item
    page.locator('.win-keep').click()
    page.wait_for_timeout(300)
    page.locator('#screen-open .back-btn').click()
    page.wait_for_timeout(300)

    # Inventory should have at least 1 item
    page.locator('#screen-home .bottom-nav .nav-item').nth(2).click()
    page.wait_for_timeout(400)
    inv_count = int(page.locator('#inv-count').inner_text())
    checks.append(('Inventory count >= 1', inv_count >= 1))
    page.screenshot(path=str(OUT / '02-inventory-after-sell.png'), full_page=True)

    # Deposit demo top-up (manual input mode)
    page.locator('#screen-inventory .header .balance-pill').click()
    page.wait_for_timeout(350)
    checks.append(('Deposit screen active', page.locator('#screen-deposit.active').count() == 1))
    page.locator('#deposit-input').fill('1000')
    page.wait_for_timeout(200)
    page.locator('#deposit-btn').click()
    page.wait_for_timeout(450)

    # verify balance increased on the currently active screen
    page.wait_for_timeout(300)
    active_screen = page.evaluate("document.querySelector('.screen.active')?.id || ''")
    if active_screen == 'screen-home':
        balance_after = parse_balance(page.locator('#balance').inner_text())
    elif active_screen == 'screen-inventory':
        balance_after = parse_balance(page.locator('#balance2').inner_text())
    else:
        balance_after = parse_balance(page.locator('#balance').inner_text())
    checks.append(('Balance increases after deposit', balance_after > start_balance))

    # Upgrades screen and action
    page.locator('.screen.active .bottom-nav .nav-item').nth(1).click()
    page.wait_for_timeout(350)
    checks.append(('Upgrades screen active', page.locator('#screen-upgrades.active').count() == 1))
    source_count = page.locator('#upgrade-source-list .up-pick').count()
    target_count = page.locator('#upgrade-target-list .up-pick').count()
    checks.append(('Upgrade source items rendered', source_count >= 1))
    checks.append(('Upgrade target items rendered', target_count >= 1))

    run_btn = page.locator('#upgrade-run-btn')
    checks.append(('Upgrade button enabled', run_btn.is_enabled()))
    page.locator('#upgrade-source-list .up-pick').first.click()
    page.wait_for_timeout(200)
    page.locator('#upgrade-target-list .up-pick').first.click()
    page.wait_for_timeout(200)
    page.locator('#upgrade-run-btn').click()
    page.wait_for_timeout(2800)
    checks.append(('Upgrade run processed', page.locator('#toast').inner_text() != ''))
    page.screenshot(path=str(OUT / '03-upgrades.png'), full_page=True)

    # Profile navigation and Steam button should not hard fail
    page.locator('.screen.active .bottom-nav .nav-item').nth(3).click()
    page.wait_for_timeout(350)
    checks.append(('Profile screen active', page.locator('#screen-profile.active').count() == 1))
    page.locator('#screen-profile .menu-item', has_text='Привязать Steam').first.click()
    page.wait_for_timeout(250)

    # Ensure toast is multiline friendly by checking computed style
    toast_ws = page.evaluate("getComputedStyle(document.getElementById('toast')).whiteSpace")
    checks.append(('Toast wraps text', toast_ws != 'nowrap'))
    page.screenshot(path=str(OUT / '04-profile-toast.png'), full_page=True)

    browser.close()

report = ['Miniapp full regression', f'URL: {URL}', '', 'Checks:']
for name, ok in checks:
    report.append(f"- {'OK' if ok else 'FAIL'}: {name}")

if errors:
    report.append('')
    report.append('Console/Page errors:')
    for e in errors:
        report.append(f'- {e}')

(OUT / 'report.txt').write_text('\n'.join(report), encoding='utf-8')
print('\n'.join(report))
print(f'\nArtifacts: {OUT}')
