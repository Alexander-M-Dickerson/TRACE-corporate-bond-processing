# Updated TRACE Cleaning and Noise Reduction 

## Overview

This Python repository creates an updated version of the cleaned daily TRACE data with best practices. It also contains scripts to construct market microstructure noise (MMN) free bond returns.

## Usage

A. Ensure you have Python installed on your system. If not, you can download it from the official Python website: [python.org](https://www.python.org/downloads/)

B. Update and install the required packages.

Run these scripts sequentially to produce the TRACE bond-level panel.

1. Run ```CleanTRACEIntraday.py```. This script outputs the daily bond-level panel with clean prices, and volumes using updated best practices.

2. Run ```MakeDailyTRACE.py```. This script outputs the daily bond accrued interest, dirty prices, duration, convexity and yields.

3. Run ```MakeMMNFreeReturns.py```. This script outputs the monthly MMN-adjusted bond returns.
