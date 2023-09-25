from bs4 import BeautifulSoup
import requests
import pandas
import json
import concurrent.futures
import time


def from_infobox_children(soup, start, search):
    result = None
    start_index = None

    trs = soup.select("table.infobox tr")

    for i, tr in enumerate(trs):
        th = tr.select_one("th")
        if th and start in th.text.strip() and not start_index:
            start_index = i
            continue
        if start_index and th and search in th.text.strip():
            td = tr.select_one("td")
            if not td:
                continue
            result = td.text.strip()
            break

    return result or " "


def from_infobox(soup, search):
    result = None

    trs = soup.select("table.infobox tr")

    for tr in trs:
        th = tr.select_one("th")
        if th and search in th.text.strip():
            td = tr.select_one("td")
            if not td:
                continue
            result = td.text.strip()
            break

    return result or " "


def to_json(data):
    with open("data.json", "w") as f:
        f.write(json.dumps(data, ensure_ascii=False))


def to_tsv(data):
    with open("data.tsv", "w") as f:
        f.write("\t".join(data[0].keys()) + "\n")
        for row in data:
            f.write("\t".join(row.values()) + "\n")


def get_wiki(url):
    url = url.replace("/de.", "/en.")
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    wiki = {}

    wiki["url"] = url

    # Get the page title
    page_title = soup.select_one("span.mw-page-title-main")
    wiki["page_title"] = page_title.text

    # Get the name
    name = soup.select_one("th.infobox-above div")
    wiki["name"] = name.text if name else ""

    # Get the city type
    city_type = soup.select_one("td.infobox-subheader div a")
    wiki["city_type"] = city_type.text if city_type else ""

    # Get the official name
    wiki["official_name"] = " "

    # Get the state
    state = from_infobox(soup, "State")
    wiki["state"] = state

    # Get the county
    county = from_infobox(soup, "County")
    wiki["county"] = county

    # Get the nickname
    nickname = from_infobox(soup, "Nickname")
    wiki["nickname"] = nickname

    # Get the motto
    motto = from_infobox(soup, "Motto")
    wiki["motto"] = motto

    # Get the population total
    population_total = from_infobox_children(soup, "Population", "Total")
    wiki["population_total"] = population_total

    # Get the population city
    population_city = from_infobox_children(soup, "Population", "City")
    wiki["population_city"] = population_city

    # Get the population estimate
    population_estimate = from_infobox_children(soup, "Population", "Estimate")
    wiki["population_estimate"] = population_estimate

    # Get the population rank
    population_rank = from_infobox_children(soup, "Population", "Rank")
    wiki["population_rank"] = population_rank

    # Get the population density
    population_density = from_infobox_children(soup, "Population", "Density")
    wiki["population_density"] = population_density

    # Get the established date
    established_date = from_infobox(soup, "Settled")
    wiki["established_date"] = established_date

    # Get the named for
    named_for = from_infobox(soup, "Named for")
    wiki["named_for"] = named_for

    # Get the government type
    government_type = from_infobox_children(soup, "Government", "Type")
    wiki["government_type"] = government_type

    # Get the government body
    government_body = from_infobox_children(soup, "Government", "Body")
    wiki["government_body"] = government_body

    # Get the area total
    area_total = from_infobox_children(soup, "Area", "Total")
    wiki["area_total"] = area_total

    # Get the area city
    area_city = from_infobox_children(soup, "Area", "City")
    wiki["area_city"] = area_city

    # Get the area land
    area_land = from_infobox_children(soup, "Area", "Land")
    wiki["area_land"] = area_land

    # Get the area water
    area_water = from_infobox_children(soup, "Area", "Water")
    wiki["area_water"] = area_water

    # Get the elevation
    elevation = from_infobox(soup, "Elevation")
    wiki["elevation"] = elevation

    # Get the highest elevation
    highest_elevation = from_infobox(soup, "Highest elevation")
    wiki["highest_elevation"] = highest_elevation

    # Get the time zone
    timezone = from_infobox(soup, "Time zone")
    wiki["timezone"] = timezone

    # Get the summer (DST)
    summer_dst = from_infobox_children(soup, "Time zone", "Summer")
    wiki["summer_dst"] = summer_dst

    # Get the ZIP code
    zip_code = from_infobox(soup, "ZIP code")
    wiki["zip_code"] = zip_code

    # Get the area code
    area_code = from_infobox(soup, "Area code")
    wiki["area_code"] = area_code

    # Get the FIPS code
    fips_code = from_infobox(soup, "FIPS code")
    wiki["fips_code"] = fips_code

    # Get the GNIS feature ID
    gnis_feature_id = from_infobox(soup, "GNIS feature ID")
    wiki["gnis_feature_id"] = gnis_feature_id

    # Get the website
    website = from_infobox(soup, "Website")
    wiki["website"] = website

    # Get the coordinates and coordinates link
    coord_span = soup.select_one("span.geo-inline")
    if coord_span:
        latitde = coord_span.select_one("span.latitude")
        longitude = coord_span.select_one("span.longitude")
        coordinates = latitde.text.strip() + " " + longitude.text.strip()
        wiki["coordinates"] = coordinates

        coord_link = coord_span.select_one("a")
        wiki["coordinates_link"] = coord_link["href"]
    else:
        wiki["coordinates"] = " "
        wiki["coordinates_link"] = " "

    for key, value in wiki.items():
        wiki[key] = value.replace("\xa0", " ")

    # Denonym

    # to_json(wiki)

    return wiki


def main():
    df = pandas.read_excel("wiki_urls.xlsx")
    urls = df.iloc[:, 1].dropna()
    data = []

    start = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for url in urls:
            futures.append(executor.submit(get_wiki, url))
        for future in concurrent.futures.as_completed(futures):
            data.append(future.result())

    data = sorted(data, key=lambda x: x['url'])

    end = time.time()
    print(end - start)

    to_tsv(data)


main()
