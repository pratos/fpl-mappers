import asyncio
import aiohttp
import pandas as pd

from bs4 import BeautifulSoup

'''
Updates the fbref_data.csv file with the data for all players found in the id_map.csv file
'''

async def fetch(session, url):
    async with session.get(url) as response:
        result = await response.read()
        return result


def get_ids(id_map, data):

    # get fpl id and fbref id from rows that contain at least one NaN
    incomplete_rows = data[data.isna().any(axis=1)]
    incomplete_rows = incomplete_rows[["fpl_id", "fbref_id"]].values.tolist()

    # get fpl id and fbref id from rows that appear in the id_map but not in data
    merged = id_map.merge(data, how="left", indicator=True)
    new_rows = merged.query("_merge == 'left_only'")
    new_rows = new_rows[["fpl_id", "fbref_id"]].values.tolist()
    
    return (incomplete_rows, new_rows)


async def get_player_data(session, ID):
    # fetch and parse player information from their fbref homepage

    url = f"https://fbref.com/en/players/{ID[1]}/"
    response = await fetch(session, url)
    soup = BeautifulSoup(response, "html.parser")

    person = soup.find("div", {"itemtype": "https://schema.org/Person"})
    nation, club = "", ""
    paragraph_tags = person.find_all("p")

    name = person.find("h1").text.strip()
    full_name = name
    poss_name = paragraph_tags[0].text.strip()

    if "Position:" not in poss_name:
        full_name = poss_name

    for p in paragraph_tags:
        data = p.find("strong")
        if data is None:
            continue

        text = data.text.strip()

        if any(["Citizenship:" in text, "National Team:" in text]):
            nation = p.find("a").text.strip()

        if text == "Club:":
            club = p.find("a").text.strip()
        
        if text == "Born:":
            dob = p.find("span", {"itemprop": "birthDate"}).text.strip()
            dob = dob.replace(",", "")

    h = person.find("span", {"itemprop": "height"})
    height = h.text.strip().split("cm")[0] if h is not None else ""
    
    w = person.find("span", {"itemprop": "weight"})
    weight = w.text.strip().split("kg")[0] if w is not None else ""
    
    return ID + [name, full_name, height, weight, dob, nation, club]


async def main():
    id_map = pd.read_csv("id_map.csv")
    data = pd.read_csv("fbref_data.csv")
    
    incomplete, new = get_ids(id_map, data)

    async with aiohttp.ClientSession() as session:
        tasks = [get_player_data(session, ID) for ID in incomplete + new]
        players = await asyncio.gather(*tasks)

    for i, player in enumerate(players):
        # replace incomplete rows in original df with newly fetched information
        if player[:2] in incomplete:
            data.loc[data["fpl_id"] == player[0]] = player
        
        # append newly added players to the bottom of the dataframe
        else:
            data = data.append(pd.DataFrame(players[i:], columns=data.columns))
            break
    
    data.to_csv("fbref_data.csv", index=False)


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
