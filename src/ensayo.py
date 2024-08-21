import pandas as pd
import json
import os
import constants

parameters_file_path = os.path.join(constants.paths["config"], "parameters.json")
with open(parameters_file_path, 'r', encoding="UTF-8") as file:
    params = json.load(file)

print(os.listdir(os.path.join(constants.paths["data"], "gkg")))
