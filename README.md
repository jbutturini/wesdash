# I. WES Market Data Generator
The WES Market Data Generator is a python script which ingests publicly available data to provide indicators of market contraction or expansion in the WES target markets. There are two primary inputs for this script:
1. **American Community Survey (ACS)**: The U.S. Census Bureau runs the ACS. It’s one of the Bureau’s core programs for “detailed characteristics” data (income, education, commuting, housing, etc.), collected continuously and released as 1-year and 5-year estimates.
2. **OSSE Chronic Absenteeism Scores**: The Office of the State Superintendent of Education for Washington, DC maintains the absenteeism data. OSSE collects certified attendance/enrollment data from schools/LEAs and then calculates the chronic absenteeism metric for the DC School Report Card and related data files.

The WES Market Data Generator takes both sources as input and generates a single Excel spreadsheet with the WES Market Data KPIs.

# II. Usage

## Refresh
The `wesdash refresh --help` command refreshes the entire `wesdash` dataset. It generates workbooks for:
* ACS 1 year
* ACS 5 year
* OSSE Absenteeism

To refresh the dataset, run the following command from a command prompt:
```
python wesdash.py refresh --geo geo.yaml
```

This writes workbooks to:
* `out/wes_kpi_acs5.xlsx`
* `out/wes_kpi_acs1.xlsx`

Each sheet contains a time series from 2015 through the latest published vintage, with a `year` column indicating the vintage end year.

## Configuration
The `geo.yaml` file specifies the geographies for which the dashboard will collect information. Geographies contain datasets, which have dataset-specific query specification configuration fields.
```
geographies:                 # required top-level configuration
  <geo-label>:               # specifies a label for the geography (e.g. moco_md)
    label: <label>           # output friendly label for the geography
    datasets:                # required top-level configuration for datasets
    - dataset: <dataset-id>  # the identifier for the dataset
      ...                    # dataset-specific configuration
```

# III. Source Reference

## American Community Survey (ACS)
ACS data is gathered every year and compiled into both a 1-year and a rolling 5-year dataset. The rolling 5 year dataset gets recomputed every year as the window moves forward. At the 5 year, releases of ACS data lag. For example, Census’s 2024 release materials show 2020–2024 ACS 5-year estimates scheduled for December 11, 2025, and the ACS “updates” page notes the next release on January 29 will include the 2020–2024 5-year estimates. The 5 year is great for small geographies (tracts, ZCTAs) because it’s stable. For small geographies (like many ZCTAs or census tracts), the number of sampled households in any single year is often too small to produce stable estimates.

The ACS data (for both 1 year and 5 year) is structured as follows:
* The table ID is the prefix (e.g., B01001, B14003, S1101, B19131).
* The suffix like _003E is a specific line (“cell”) in that table.
* E = estimate (the point estimate). There are also M margin-of-error variables and EA/MA “annotation” helpers in the API output.

### Sex by Age (B01001)
These are population counts by sex and age band. API reference: https://api.census.gov/data/2022/acs/acs5/groups/B01001.html

**Age 0-4**
* `B01001_003E`: _Estimate!!Total:!!Male:!!Under 5 years_
* `B01001_027E`: _Estimate!!Total:!!Female:!!Under 5 years_

_Motivation_: summed together, these give a clean Age 0–4 child count (male + female). That’s a forward-looking pipeline indicator for PreK/early-entry demand.

**Age 5-9**
* `B01001_004E`: _Estimate!!Total:!!Male:!!5 to 9 years_
* `B01001_028E`: _Estimate!!Total:!!Female:!!5 to 9 years_

_Motivation_: summed together, these produce Age 5–9 child count, a rough proxy for K–3/4-aged children in the area (depending on cutoff assumptions).

**Age 10-14**
* `B01001_005E`: _Estimate!!Total:!!Male:!!10 to 14 years_
* `B01001_029E`: _Estimate!!Total:!!Female:!!10 to 14 years_

_Motivation_: summed together, these produce Age 10–14 child count, which is a proxy for upper elementary / middle-school-aged children (useful for forecasting later-grade demand).

### Households with Kids (S1101)
These are households with children under 18. API reference: https://api.census.gov/data/2012/acs/acs5/subject/variables/S1101_C01_005MA.html
* `S1101_C01_005E`: _Annotation of Total!!Margin of Error!!AGE OF OWN CHILDREN!!Households with own children under 18 years_

_Motivation_: this is the cleanest “how many households have kids” signal (as opposed to just “how many kids exist”). For an independent school lens, this is often closer to the addressable family market than raw child counts.

### High-Income Households with Kids (B19131)
B19131 gives "Family Type by Presence of Own Children Under 18 Years by Family Income in the Past 12 Months." API reference: https://api.census.gov/data/2022/acs/acs5/variables/B19131_028MA.html
* `B19131`: the script filters on labels which include `"$150,000 to $199,999"` and `"$200,000 or more"`

### School enrollment by Sex, School Type, and Age
This table is explicitly about enrollment and distinguishes public vs private. API reference: https://api.census.gov/data/2022/acs/acs5/groups/B14003.html

**Public School**
* `B14003_004E`: _Estimate!!Total:!!Male:!!Enrolled in public school:!!3 and 4 years_
* `B14003_005E`: _Estimate!!Total:!!Male:!!Enrolled in public school:!!5 to 9 years_
* `B14003_006E`: _Estimate!!Total:!!Male:!!Enrolled in public school:!!10 to 14 years_
* `B14003_032E`: _Estimate!!Total:!!Female:!!Enrolled in public school:!!3 and 4 years_
* `B14003_033E`: _Estimate!!Total:!!Female:!!Enrolled in public school:!!5 to 9 years_
* `B14003_034E`: _Estimate!!Total:!!Female:!!Enrolled in public school:!!10 to 14 years_

**Private School**
* `B14003_013E`: _Estimate!!Total:!!Male:!!Enrolled in private school:!!3 and 4 years_
* `B14003_014E`: _Estimate!!Total:!!Male:!!Enrolled in private school:!!5 to 9 years_
* `B14003_015E`: _Estimate!!Total:!!Male:!!Enrolled in private school:!!10 to 14 years_
* `B14003_041E`: _Estimate!!Total:!!Female:!!Enrolled in private school:!!3 and 4 years_
* `B14003_042E`: _Estimate!!Total:!!Female:!!Enrolled in private school:!!5 to 9 years_
* `B14003_043E`: _Estimate!!Total:!!Female:!!Enrolled in private school:!!10 to 14 years_

_Motivation_: these are the ACS-native way to estimate the private school chooser rate in the age bands that overlap with K–8 demand, which is given as `chooser % = private / (private + public)`.
