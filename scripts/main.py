#%%
from modules_database import *
from modules_core import *
import os

setup_database()
load_entities_from_csv()
tokenize()
#jaccard_similarity()
soft_jaccard_similarity()
pairs_validation()

print("All processes completed successfully.")

# %%
