# Load libraries #
import pandas as pd
import numpy as np
import urllib.request
import zipfile

# Load data      #
bbw_url = str("https://openbondassetpricing.com/wp-content/uploads/2023/04/")+\
str("bbw_trace_2023.zip")

# Download the file and save it
# We will name it bbw_trace_2023.zip file

urllib.request.urlretrieve(bbw_url,'bbw_trace_2023.zip')
zip_file = zipfile.ZipFile('bbw_trace_2023.zip', 'r')

# Next we extact the file data
zip_file.extractall()
# Make sure you close the file after extraction
zip_file.close()

# Store the factors in bbw_factors
bbw_factors = pd.read_csv('bbw_trace_2023.csv', skiprows = 0)

# End #
