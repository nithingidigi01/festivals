import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import json
import re

YEAR = 2026

BASE_URL = "https://www.drikpanchang.com"
MONTH_URL = "https://www.drikpanchang.com/festivals/month/festivals-{}.html?year=2026"

MONTHS = [
    "january","february","march","april",
    "may","june","july","august",
    "september","october","november","december"
]

# -------------------------------
# CLASSIFY FOR PELLIMELAM
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
# EXTRACT LIST (1000+)
# -------------------------------
def extract_list(soup, url):
    text = soup.get_text("\n")
    lines = [l.strip() for l in text.split("\n") if l.strip()]

    festivals = []

    for i in range(len(lines)):
        line = lines[i]

        if "2026" in line and "," in line:
            try:
                name = lines[i-1]

                festivals.append({
                    "name": name,
                    "date_raw": line,
                    "month_source": url
                })
            except:
                continue

    return festivals


# -------------------------------
# EXTRACT DETAILS (IF AVAILABLE)
# -------------------------------
def extract_details(soup):
    text = soup.get_text("\n")
    lines = [l.strip() for l in text.split("\n") if l.strip()]

    data = {}

    # Tithi
    for line in lines:
        if "Tithi Begins" in line:
            data["tithi_start"] = line
        if "Tithi Ends" in line:
            data["tithi_end"] = line

    # Moonrise
    for line in lines:
        if "Moonrise" in line:
            data["moonrise"] = line

    # Description
    desc = []
    capture = False

    for line in lines:
        if len(line) > 50:
            desc.append(line)
        if len(desc) > 10:
            break

    data["description"] = " ".join(desc)

    return data


# -------------------------------
# MAIN
# -------------------------------
async def main():
    all_festivals = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        # STEP 1: Collect all festivals
        for month in MONTHS:
            url = MONTH_URL.format(month)
            print(f"\nFetching {month}...")

            await page.goto(url)
            await page.wait_for_timeout(5000)

            html = await page.content()
            soup = BeautifulSoup(html, "lxml")

            data = extract_list(soup, url)
            all_festivals.extend(data)

        print(f"\nCollected {len(all_festivals)} raw festivals")

        # STEP 2: Try enriching details (LIMITED for performance)
        for i, fest in enumerate(all_festivals[:300]):  # limit for speed
            name = fest["name"]

            print(f"Enriching {i+1}/{len(all_festivals)}")

            try:
                search_url = f"https://www.drikpanchang.com/search?q={name.replace(' ', '+')}"
                await page.goto(search_url)
                await page.wait_for_timeout(3000)

                link = await page.evaluate("""
                () => {
                    let a = document.querySelector("a[href*='date-time']");
                    return a ? a.href : null;
                }
                """)

                if link:
                    await page.goto(link)
                    await page.wait_for_timeout(3000)

                    html = await page.content()
                    soup = BeautifulSoup(html, "lxml")

                    details = extract_details(soup)

                    fest.update(details)

            except:
                continue

        await browser.close()

    # remove duplicates
    unique = {}
    for f in all_festivals:
        key = (f["name"], f["date_raw"])
        unique[key] = f

    final_data = list(unique.values())

    # classify
    for f in final_data:
        f["pellimelam_use"] = classify_use(
            f.get("name",""),
            f.get("description","")
        )

    # save
    with open("hindu_festivals_2026_full.json", "w", encoding="utf-8") as f:
        json.dump(final_data, f, indent=2, ensure_ascii=False)

    print(f"\n✅ FINAL DATASET: {len(final_data)} festivals saved")


asyncio.run(main())