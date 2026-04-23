import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import json
import re

BASE = "https://www.drikpanchang.com"
MONTH_URL = BASE + "/festivals/month/festivals-{}.html?year=2026"

MONTHS = [
    "january","february","march","april",
    "may","june","july","august",
    "september","october","november","december"
]

SEM_LIMIT = 8


# -------------------------------
# CLEAN TEXT
# -------------------------------
def clean(text):
    return re.sub(r"\s+", " ", text).strip()


# -------------------------------
# STEP 1: EXTRACT LIST (ROBUST)
# -------------------------------
def extract_list(soup, month):
    festivals = []

    text = soup.get_text("\n")
    lines = [l.strip() for l in text.split("\n") if l.strip()]

    for i in range(len(lines)):
        if "2026" in lines[i] and "," in lines[i]:
            name = lines[i-1]

            if len(name) < 3:
                continue

            festivals.append({
                "name": clean(name),
                "date": clean(lines[i]),
                "month": month.capitalize()
            })

    return festivals


# -------------------------------
# STEP 2: FIND DETAIL PAGE VIA SEARCH
# -------------------------------
async def get_detail_url(page, name):
    try:
        search_url = f"{BASE}/search?q={name.replace(' ', '+')}"
        await page.goto(search_url, timeout=60000)

        # find first valid festival link
        link = await page.evaluate("""
        () => {
            let links = document.querySelectorAll("a");
            for (let a of links) {
                let href = a.href;
                if (href.includes("/vrat/") || 
                    href.includes("/sankranti/") || 
                    href.includes("/festivals/")) {
                    return href;
                }
            }
            return null;
        }
        """)

        return link

    except:
        return None


# -------------------------------
# STEP 3: EXTRACT DETAILS
# -------------------------------
def extract_details(soup):
    data = {}

    text = soup.get_text("\n")
    lines = [l.strip() for l in text.split("\n") if l.strip()]

    for line in lines:
        if "Tithi Begins" in line:
            data["tithi_start"] = line.split("-")[-1].strip()

        if "Tithi Ends" in line:
            data["tithi_end"] = line.split("-")[-1].strip()

        if "Moonrise" in line:
            data["moonrise"] = line.split("-")[-1].strip()

    desc = []
    for p in soup.find_all("p"):
        txt = clean(p.get_text())

        if (
            len(txt) > 100
            and "timings are represented" not in txt.lower()
            and "discover more" not in txt.lower()
        ):
            desc.append(txt)

        if len(desc) >= 2:
            break

    data["description"] = " ".join(desc)

    return data


# -------------------------------
# STEP 4: PROCESS ONE FESTIVAL
# -------------------------------
async def process_festival(browser, fest, sem):
    async with sem:
        try:
            page = await browser.new_page()

            # get detail page
            url = await get_detail_url(page, fest["name"])
            fest["detail_url"] = url

            if url:
                await page.goto(url, timeout=60000)
                soup = BeautifulSoup(await page.content(), "lxml")

                fest.update(extract_details(soup))

            await page.close()

        except:
            pass


# -------------------------------
# MAIN
# -------------------------------
async def main():
    all_festivals = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        context = await browser.new_context(
            locale="en-IN",
            timezone_id="Asia/Kolkata"
        )

        page = await context.new_page()

        # STEP 1: COLLECT ALL FESTIVALS
        for month in MONTHS:
            url = MONTH_URL.format(month)
            print(f"Fetching {month}...")

            await page.goto(url, timeout=60000)
            await page.wait_for_load_state("networkidle")

            soup = BeautifulSoup(await page.content(), "lxml")
            data = extract_list(soup, month)

            all_festivals.extend(data)

        print(f"\nCollected: {len(all_festivals)}")

        # STEP 2: PARALLEL DETAIL EXTRACTION
        sem = asyncio.Semaphore(SEM_LIMIT)
        tasks = [process_festival(browser, f, sem) for f in all_festivals[:300]]

        await asyncio.gather(*tasks)

        await browser.close()

    # SAVE
    with open("hindu_festivals_2026_full.json", "w", encoding="utf-8") as f:
        json.dump(all_festivals, f, indent=2, ensure_ascii=False)

    print(f"\n✅ DONE: {len(all_festivals)} festivals")


asyncio.run(main())
