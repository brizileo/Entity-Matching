#%%
from modules_database import *
from modules_core import *
from tests_control import *
import os

setup_database()
load_entities_from_csv() #Adapt this function to your CSV file
tokenize()
#jaccard_similarity()   # Prefer soft Jaccard Uncomment this line if you want to compute Jaccard similarity
soft_jaccard_similarity()
pairs_validation()
report()

print("All processes completed successfully.")

# %%
