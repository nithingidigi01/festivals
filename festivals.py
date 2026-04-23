import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import json
import re

YEAR = 2026
BASE = "https://www.drikpanchang.com"
MONTH_URL = BASE + "/festivals/month/festivals-{}.html?year=2026"

MONTHS = [
    "january","february","march","april",
    "may","june","july","august",
    "september","october","november","december"
]

SEM_LIMIT = 10


# -------------------------------
# CLASSIFY
# -------------------------------
def classify_use(name, desc):
    text = (name + " " + desc).lower()

    if any(x in text for x in ["pooja","puja","temple","jayanti"]):
        return "Temple pooja / devotional music"

    if any(x in text for x in ["festival","utsav","celebration"]):
        return "Festival celebration / procession"

    if any(x in text for x in ["wedding","vivah"]):
        return "Wedding ceremony"

    return "General religious event"


# -------------------------------
# CLEAN TEXT
# -------------------------------
def clean_text(text):
    text = re.sub(r"\s+", " ", text)
    return text.strip()


# -------------------------------
# EXTRACT FESTIVAL LIST (CLEAN)
# -------------------------------
def extract_list(soup, url, month):
    festivals = []

    rows = soup.select("table tr")

    for row in rows:
        cols = [c.strip() for c in row.get_text("\n").split("\n") if c.strip()]

        if len(cols) >= 2 and "2026" in cols[1]:
            festivals.append({
                "name": clean_text(cols[0]),
                "date_raw": clean_text(cols[1]),
                "month": month.capitalize(),
                "detail_url": None,
                "source": url
            })

    # attach detail links
    for a in soup.find_all("a", href=True):
        if "-date-time" in a["href"]:
            name = clean_text(a.get_text())

            for f in festivals:
                if name.lower() in f["name"].lower():
                    f["detail_url"] = BASE + a["href"]

    return festivals


# -------------------------------
# EXTRACT DETAILS (STRICT + CLEAN)
# -------------------------------
def extract_details(soup):
    data = {}

    text = soup.get_text("\n")
    lines = [l.strip() for l in text.split("\n") if l.strip()]

    # -------------------
    # DATE
    # -------------------
    for line in lines:
        if "2026" in line and "," in line:
            data["date_full"] = line
            break

    # -------------------
    # TIMINGS (STRICT MATCH)
    # -------------------
    for line in lines:
        if "Tithi Begins" in line:
            data["tithi_start"] = line.split("-")[-1].strip()

        if "Tithi Ends" in line:
            data["tithi_end"] = line.split("-")[-1].strip()

        if "Moonrise" in line:
            data["moonrise"] = line.split("-")[-1].strip()

    # -------------------
    # DESCRIPTION (FILTERED)
    # -------------------
    desc = []

    for p in soup.find_all("p"):
        txt = clean_text(p.get_text())

        if (
            len(txt) > 120
            and "timings are represented" not in txt.lower()
            and "choose year" not in txt.lower()
            and "discover more" not in txt.lower()
            and "copyright" not in txt.lower()
        ):
            desc.append(txt)

        if len(desc) >= 2:
            break

    data["description"] = " ".join(desc)

    return data


# -------------------------------
# ENRICH ONE
# -------------------------------
async def enrich(browser, fest, sem):
    async with sem:
        try:
            if not fest.get("detail_url"):
                return

            page = await browser.new_page()
            await page.goto(fest["detail_url"], timeout=60000)

            html = await page.content()
            soup = BeautifulSoup(html, "lxml")

            details = extract_details(soup)
            fest.update(details)

            await page.close()

        except:
            pass


async def enrich_all(browser, festivals):
    sem = asyncio.Semaphore(SEM_LIMIT)

    tasks = [enrich(browser, f, sem) for f in festivals if f.get("detail_url")]
    await asyncio.gather(*tasks)


# -------------------------------
# MAIN
# -------------------------------
async def main():
    all_festivals = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        # INDIA CONTEXT (IMPORTANT)
        context = await browser.new_context(
            locale="en-IN",
            timezone_id="Asia/Kolkata"
        )

        page = await context.new_page()

        # STEP 1: COLLECT
        for month in MONTHS:
            url = MONTH_URL.format(month)
            print(f"Fetching {month}...")

            await page.goto(url)
            await page.wait_for_timeout(3000)

            soup = BeautifulSoup(await page.content(), "lxml")
            data = extract_list(soup, url, month)

            all_festivals.extend(data)

        print(f"\nCollected: {len(all_festivals)}")

        # STEP 2: ENRICH
        await enrich_all(browser, all_festivals)

        await browser.close()

    # STEP 3: REMOVE DUPLICATES
    unique = {}
    for f in all_festivals:
        key = (f["name"], f["date_raw"])
        unique[key] = f

    final = list(unique.values())

    # STEP 4: CLASSIFY
    for f in final:
        f["category"] = classify_use(
            f.get("name",""),
            f.get("description","")
        )

    # STEP 5: FINAL STRUCTURE
    cleaned = []

    for f in final:
        cleaned.append({
            "name": f.get("name"),
            "date": f.get("date_raw"),
            "month": f.get("month"),
            "tithi_start": f.get("tithi_start"),
            "tithi_end": f.get("tithi_end"),
            "moonrise": f.get("moonrise"),
            "description": f.get("description"),
            "category": f.get("category"),
            "detail_url": f.get("detail_url")
        })

    # SAVE
    with open("hindu_festivals_2026_clean.json", "w", encoding="utf-8") as f:
        json.dump(cleaned, f, indent=2, ensure_ascii=False)

    print(f"\n✅ FINAL CLEAN DATA: {len(cleaned)} festivals")


asyncio.run(main())
