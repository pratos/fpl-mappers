import json
import pandas as pd

from pathlib import Path
from urllib.request import urlopen

positions = {1: "G", 2: "D", 3: "M", 4:"F"}

team_data_filepath = Path.cwd().parent.joinpath("teams.json")
with open(team_data_filepath) as f:
    team_data = json.load(f)

url = "https://fantasy.premierleague.com/api/bootstrap-static/"
players = json.loads(urlopen(url).read())["elements"]

df = pd.DataFrame(data=players)
df = df.sort_values(by=["id"]).rename({"id": "fpl_id"}, axis=1)

df["position"] = df["element_type"].apply(lambda x: positions[x])
df["team"] = df["team"].apply(lambda x: [y["fpl_name"] for y in team_data if y["fpl_id"] == x][0])
df["start_cost"] = df.apply(lambda x: (x["now_cost"] - x["cost_change_start"]) / 10, axis=1)
df["now_cost"] /= 10

df = df[["fpl_id", "first_name", "second_name", "web_name", "position", "team", "start_cost", "now_cost"]]

df.to_csv("fpl_data.csv", index=False)
