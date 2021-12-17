# NREL EVI-FAST  
  
This analysis uses the Electric Vehicle Infrastructure – Financial Analysis Scenario (EVI-FAST) tool, which is publicly available [online](https://www.nrel.gov/transportation/evi-fast.html). Since registration to download is required, follow the below instructions to complete the setup of the EVI-FAST tool.
1. Download the EVI-FAST tool and save to lcoc-ldevs/evi-fast-tool
2. Rename as 'evi-fast_dcfc_lcoc'
3. In the 'evi-fast_dcfc_lcoc' workbook, select the Advanced interface. Add references to cells in lcoc_county_dcfc.xlsm (sheet EVI-FAST_calc, see notes in workbook).
4. In lcoc_county_dcfc.xlsm:
    - Sheet 'EVI-FAST_calc': Add reference to EVI-FAST output (see note in workbook).
    - Confirm connections to external data sources. Data > Queries & Connections
        - dcfc_counties_p1 through _p4: open Query in Power Query Editor, under Applied Steps, go to Source settings. Can browse to correct file (path should be something along the lines of "C:\Users\USERNAME\Documents\mapping_ev_impacts_public\02_analysis\lcoc-ldevs\outputs\cost-of-electricity\urdb-dcfc-counties\dcfc_counties_p1.csv")

5. The macro in lcoc_county_dcfc.xlsm will be triggered with code from the `dcfc_county_to_lcoc_evifast` function in the processing.py module.
