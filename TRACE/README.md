# Python Script: Data Cleaning for Trace Enhanced Database

## Overview

This Python repository creates the TRACE bond database as used in Dickerson, Mueller and Robotti (2023). It first aims to clean various data issues in the daily Trace Enhanced File. 
The cleaning procedure is largely based on the discussions presented in the works of Dick-Nielsen (2009) and (2014), 
as well as Bai, Bali, and Wen (2019). The script effectively handles Cancellation, Correction, Reversal, and Double entries in the daily data.
Moreover, it is designed to cater to both pre and post the 2012/02/06 change in the Trace System.
Thereafter, several scripts are available to create the monthly bond-level panel which variables included such as bond returns, credit spreads and others.

## Requirements

- Python 3.9.13
- pandas 
- numpy
- quantLib 1.29
- joblib 1.1.1
- wrds 3.1.2 (and access to the WRDS database and cloud)

## Usage

A. Ensure you have Python installed on your system. If not, you can download it from the official Python website: [python.org](https://www.python.org/downloads/)

B. Update and install the required packages.

Run these scripts sequentially to produce the TRACE bond-level panel.

1. Run ```MakeIntra_Daily.py```. This script outputs the daily bond-level panel with clean prices, and volumes.

2. Run ```MakeBondDailyMetrics.py```. This script outputs the daily bond accrued interest, dirty prices, duration, convexity and yields.

3. Run ```MakeBondDailyMetrics.py```. This script outputs the monthly bond returns, excess returns, bond yields, duration and convexity.

4. Run ```MakeCreditSpreads.py```. This script estimates monthly bond credit spreads.

5. Run ```MakeIlliquidity.py```. This script estimates monthly bond illiquidity following Bao et al. (2011).

6. Run ```MakeRatings.py``` and ```MakeAmountOutstanding.py``` in any order. This script downloads bond ratings and bond amount outstanding.

7. Run ```MakeDataBaseTRACE.py```This script downloads the data processed in the prior scripts and generates the final database.

## Acknowledgements

1. Matthias Buchner (Trafigura)
2. Francis Cong (Morgan Stanley)
3. Mihai Mihut (Tilburg)

