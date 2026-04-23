import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import json

YEAR = 2026
MONTH_URL = "https://www.drikpanchang.com/festivals/month/festivals-{}.html?year=2026"

MONTHS = [
    "january","february","march","april",
    "may","june","july","august",
    "september","october","november","december"
]

SEM_LIMIT = 10  # parallel workers


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
# EXTRACT LIST + DIRECT LINKS
# -------------------------------
def extract_list(soup, url):
    festivals = []

    for a in soup.find_all("a", href=True):
        href = a["href"]

        # direct detail page links
        if "-date-time" in href:
            name = a.get_text(strip=True)

            festivals.append({
                "name": name,
                "detail_url": "https://www.drikpanchang.com" + href,
                "month_source": url
            })

    # fallback for rows without links
    text = soup.get_text("\n")
    lines = [l.strip() for l in text.split("\n") if l.strip()]

    for i in range(len(lines)):
        line = lines[i]

        if "2026" in line and "," in line:
            name = lines[i-1]

            festivals.append({
                "name": name,
                "date_raw": line,
                "month_source": url
            })

    return festivals


# -------------------------------
# EXTRACT FULL DETAILS
# -------------------------------
def extract_details(soup):
    data = {}

    # TITLE
    h1 = soup.find("h1")
    if h1:
        data["title"] = h1.get_text(strip=True)

    # TIMINGS
    timings = []
    for tag in soup.find_all(["p", "div"]):
        text = tag.get_text(" ", strip=True)

        if any(x in text for x in [
            "Tithi Begins",
            "Tithi Ends",
            "Moonrise",
            "Jayanti on",
            "Timings"
        ]):
            timings.append(text)

    data["timings"] = timings

    # DESCRIPTION (clean)
    description = []
    for p in soup.find_all("p"):
        text = p.get_text(" ", strip=True)

        if len(text) > 80 and "timings are represented" not in text.lower():
            description.append(text)

        if len(description) >= 3:
            break

    data["description"] = " ".join(description)

    return data


# -------------------------------
# ENRICH ONE
# -------------------------------
async def enrich_festival(browser, fest, sem):
    async with sem:
        try:
            page = await browser.new_page()

            link = fest.get("detail_url")

            # fallback search if no direct link
            if not link:
                search_url = f"https://www.drikpanchang.com/search?q={fest['name'].replace(' ', '+')}"
                await page.goto(search_url)

                link = await page.evaluate("""
                () => {
                    let links = document.querySelectorAll("a");
                    for (let a of links) {
                        if (a.href.includes("-date-time")) return a.href;
                    }
                    return null;
                }
                """)

            if link:
                await page.goto(link)

                html = await page.content()
                soup = BeautifulSoup(html, "lxml")

                details = extract_details(soup)
                fest.update(details)

            await page.close()

        except:
            pass


async def enrich_all(browser, festivals):
    sem = asyncio.Semaphore(SEM_LIMIT)

    tasks = [
        enrich_festival(browser, fest, sem)
        for fest in festivals
    ]

    await asyncio.gather(*tasks)


# -------------------------------
# MAIN
# -------------------------------
async def main():
    all_festivals = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        # STEP 1: COLLECT ALL FESTIVALS
        for month in MONTHS:
            url = MONTH_URL.format(month)
            print(f"Fetching {month}...")

            await page.goto(url)
            await page.wait_for_timeout(4000)

            html = await page.content()
            soup = BeautifulSoup(html, "lxml")

            data = extract_list(soup, url)
            all_festivals.extend(data)

        print(f"\nCollected {len(all_festivals)} festivals")

        # STEP 2: ENRICH ALL (parallel)
        await enrich_all(browser, all_festivals)

        await browser.close()

    # REMOVE DUPLICATES
    unique = {}
    for f in all_festivals:
        key = f.get("detail_url") or (f.get("name"), f.get("date_raw"))
        unique[key] = f

    final = list(unique.values())

    # CLASSIFY
    for f in final:
        f["pellimelam_use"] = classify_use(
            f.get("name",""),
            f.get("description","")
        )

    # SAVE
    with open("hindu_festivals_2026_full.json", "w", encoding="utf-8") as f:
        json.dump(final, f, indent=2, ensure_ascii=False)

    print(f"\n✅ FINAL: {len(final)} festivals saved")


asyncio.run(main())
