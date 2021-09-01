import pandas as pd
from pathlib import Path

cwd = Path().cwd()

fpl = pd.read_csv(cwd.joinpath("FPL", "fpl_data.csv"))

fbref = pd.read_csv(cwd.joinpath("FBref", "fbref_data.csv"))[["fpl_id", "fbref_id"]]

# dtype=Int64 allows the columns to be treated as ints while ignoring NaN values
# this is done so the understat_id column doesn't get cast from int to float
understat = pd.read_csv(cwd.joinpath("Understat", "id_map.csv"), dtype="Int64")
understat = understat[["fpl_id", "understat_id"]]

master = fpl
for df in [fbref, understat]:
    master = master.merge(df, how="left", on="fpl_id")

master.to_csv("master.csv", index=False)
