#%%
from modules_database import *
from modules_core import *
import os


setup_database()
load_entities_from_csv()
identify_candidate_pairs()

# %%
