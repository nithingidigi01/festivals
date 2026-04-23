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
# EXTRACT FESTIVAL LIST
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
                "month": month.capitalize(),
                "detail_url": None,
                "tithi_start": None,
                "tithi_end": None,
                "moonrise": None,
                "description": None
            })

    return festivals


# -------------------------------
# GET VALID DETAIL URL
# -------------------------------
async def get_detail_url(page, name):
    try:
        search_url = f"{BASE}/search?q={name.replace(' ', '+')}"
        await page.goto(search_url, timeout=60000)

        links = await page.evaluate("""
        () => {
            let results = [];
            document.querySelectorAll("a").forEach(a => {
                if (a.href && a.innerText) {
                    results.push({
                        text: a.innerText.trim(),
                        href: a.href
                    });
                }
            });
            return results;
        }
        """)

        valid = []

        for l in links:
            href = l["href"]

            if (
                ("-date-time" in href or "-dates" in href)
                and "calendar" not in href
                and "panchang" not in href
            ):
                valid.append(l)

        # Best match
        for l in valid:
            if name.lower() in l["text"].lower():
                return l["href"]

        if valid:
            return valid[0]["href"]

        return None

    except:
        return None


# -------------------------------
# EXTRACT DETAILS
# -------------------------------
def extract_details(soup):
    data = {
        "tithi_start": None,
        "tithi_end": None,
        "moonrise": None,
        "description": None
    }

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
            len(txt) > 120
            and "timings are represented" not in txt.lower()
            and "discover more" not in txt.lower()
            and "copyright" not in txt.lower()
        ):
            desc.append(txt)

        if len(desc) >= 2:
            break

    if desc:
        data["description"] = " ".join(desc)

    return data


# -------------------------------
# PROCESS FESTIVAL
# -------------------------------
async def process_festival(browser, fest, sem):
    async with sem:
        try:
            page = await browser.new_page()

            url = await get_detail_url(page, fest["name"])

            # strict validation
            if url and not ("-date-time" in url or "-dates" in url):
                url = None

            fest["detail_url"] = url

            if url:
                await page.goto(url, timeout=60000)
                await page.wait_for_load_state("networkidle")

                soup = BeautifulSoup(await page.content(), "lxml")
                details = extract_details(soup)

                fest.update(details)

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

        # STEP 1: COLLECT
        for month in MONTHS:
            url = MONTH_URL.format(month)
            print(f"Fetching {month}...")

            await page.goto(url, timeout=60000)
            await page.wait_for_load_state("networkidle")

            soup = BeautifulSoup(await page.content(), "lxml")
            data = extract_list(soup, month)

            all_festivals.extend(data)

        print(f"\nCollected: {len(all_festivals)}")

        # STEP 2: ENRICH
        sem = asyncio.Semaphore(SEM_LIMIT)
        tasks = [process_festival(browser, f, sem) for f in all_festivals[:300]]

        await asyncio.gather(*tasks)

        await browser.close()

    # FINAL CLEAN JSON
    final = []
    for f in all_festivals:
        final.append({
            "name": f["name"],
            "date": f["date"],
            "month": f["month"],
            "tithi_start": f["tithi_start"],
            "tithi_end": f["tithi_end"],
            "moonrise": f["moonrise"],
            "description": f["description"],
            "detail_url": f["detail_url"]
        })

    with open("hindu_festivals_2026_full.json", "w", encoding="utf-8") as f:
        json.dump(final, f, indent=2, ensure_ascii=False)

    print(f"\n✅ FINAL JSON CREATED: {len(final)} festivals")


asyncio.run(main())
