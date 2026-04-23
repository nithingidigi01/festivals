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
# CLEAN TEXT
# -------------------------------
def clean_text(text):
    return re.sub(r"\s+", " ", text).strip()


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
# EXTRACT LIST (ROBUST)
# -------------------------------
from difflib import SequenceMatcher

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()


def extract_list(soup, url, month):
    festivals = []

    # STEP 1: extract festival list
    text = soup.get_text("\n")
    lines = [l.strip() for l in text.split("\n") if l.strip()]

    for i in range(len(lines)):
        line = lines[i]

        if "2026" in line and "," in line:
            try:
                name = lines[i-1]

                if len(name) < 3:
                    continue

                festivals.append({
                    "name": clean_text(name),
                    "date_raw": clean_text(line),
                    "month": month.capitalize(),
                    "detail_url": None,
                    "source": url
                })
            except:
                continue

    # STEP 2: collect ALL valid links
    link_map = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = clean_text(a.get_text())

        if not text or len(text) < 3:
            continue

        # accept all meaningful festival links
        if any(x in href for x in ["/vrat/", "/sankranti/", "/festivals/"]):
            full_url = BASE + href
            link_map.append((text.lower(), full_url))

    # STEP 3: smart matching (fuzzy)
    for f in festivals:
        fname = f["name"].lower()

        best_score = 0
        best_url = None

        for lname, url_link in link_map:
            score = similar(fname, lname)

            if score > best_score:
                best_score = score
                best_url = url_link

        # threshold to avoid wrong mapping
        if best_score > 0.6:
            f["detail_url"] = best_url

    return festivals

# -------------------------------
# EXTRACT DETAILS (CLEAN)
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
        txt = clean_text(p.get_text())

        if (
            len(txt) > 120
            and "timings are represented" not in txt.lower()
            and "discover more" not in txt.lower()
        ):
            desc.append(txt)

        if len(desc) >= 2:
            break

    data["description"] = " ".join(desc)

    return data


# -------------------------------
# ENRICH
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

            fest.update(extract_details(soup))

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

            # 🔥 CRITICAL FIX
            await page.wait_for_load_state("networkidle")

            html = await page.content()
            soup = BeautifulSoup(html, "lxml")

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

    # STEP 5: SAVE (IMPORTANT SAME NAME)
    with open("hindu_festivals_2026_full.json", "w", encoding="utf-8") as f:
        json.dump(final, f, indent=2, ensure_ascii=False)

    print(f"\n✅ FINAL DATA: {len(final)} festivals saved")


asyncio.run(main())
