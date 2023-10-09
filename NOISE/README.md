# Updated TRACE Cleaning and Noise Reduction 

## Overview

This Python repository creates an updated version of the cleaned daily TRACE data with best practices. It also contains scripts to construct market microstructure noise (MMN) free bond returns.

## Data
The pre-processed bond level panel constructed with best practices is available for download on: https://openbondassetpricing.com/, specifically, you can download the data here: 
https://openbondassetpricing.com/wp-content/uploads/2023/10/WRDS_MMN_Corrected_Data.csv.zip.

## Impact of Market Microstructure Noise
Run the script ```Make_MMNComparison.py``` to see the effects of MMN for average decile portfolio returns sorted on MMN-adjusted and unadjusted credit spread, yields, bond returns, and bond market capitlization.

## Usage

A. Ensure you have Python installed on your system. If not, you can download it from the official Python website: [python.org](https://www.python.org/downloads/)

B. Update and install the required packages.

Run these scripts sequentially to produce the TRACE bond-level panel.

1. Run ```CleanTRACEIntraday.py```. This script outputs the daily bond-level panel with clean prices, and volumes using updated best practices.

2. Run ```MakeDailyTRACE.py```. This script outputs the daily bond accrued interest, dirty prices, duration, convexity and yields.

3. Run ```MakeMMNFreeReturns.py```. This script outputs the monthly MMN-adjusted bond returns.
