import pandas as pd
from pathlib import Path

fpl = pd.read_csv(Path().cwd().joinpath("FPL", "fpl_data.csv"))

fbref = pd.read_csv(Path().cwd().joinpath("FBref", "fbref_data.csv"))
fbref = fbref[["fpl_id", "fbref_id"]]

understat = pd.read_csv(Path().cwd().joinpath("Understat", "id_map.csv"))
understat = understat[["fpl_id", "understat_id"]]

master = fpl
for df in [fbref, understat]:
    master = master.merge(df, how="left", on="fpl_id")

master.to_csv("master.csv", index=False)
