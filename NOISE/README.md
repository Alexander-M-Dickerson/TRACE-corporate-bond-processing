# Enhanced TRACE Database 

## Overview

This Python repository creates the TRACE bond database as used in Dickerson, Mueller and Robotti (2023). It first aims to clean various data issues in the daily Trace Enhanced File. 
The cleaning procedure is largely based on the discussions presented in the works of Dick-Nielsen (2009) and (2014), 
as well as Bai, Bali, and Wen (2019). The script effectively handles Cancellation, Correction, Reversal, and Double entries in the daily data.
Moreover, it is designed to cater to both pre and post the 2012/02/06 change in the Trace System.
Thereafter, several scripts are available to create the monthly bond-level panel which variables included such as bond returns, credit spreads and others.
