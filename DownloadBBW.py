# Load libraries #
import pandas as pd
import numpy as np
import urllib.request

# Load data      #
_url = 'https://openbondassetpricing.com/wp-content/uploads/2023/09/bbw_wrds_sept_2023_lastest.csv'
all_factors = pd.read_csv(_url)

# End #
