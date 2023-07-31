# Python Script: Data Cleaning for Trace Enhanced Database

## Overview

This Python script aims to clean various data issues in the Trace Enhanced File. The cleaning procedure is largely based on the discussions presented in the works of Dick-Nielsen (2009) and (2014), 
as well as Bai, Bali, and Wen (2019). The script effectively handles Cancellation, Correction, Reversal, and Double entries in the data.
Moreover, it is designed to cater to both pre and post the 2012/02/06 change in the Trace System.

## Requirements

- Python 3.9.13
- pandas 
- numpy
- quantLib 1.29
- joblib 1.1.1
- wrds 3.1.2 (and access to the WRDS database and cloud)

## Usage

1. Ensure you have Python installed on your system. If not, you can download it from the official Python website: [python.org](https://www.python.org/downloads/)

2. Update and install the required packages.

3. Run ```pip install pandas'''

