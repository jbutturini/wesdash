# I. WES Board Market Dashboard Refresh

The WES Board Market Dashboard Refresh is a python script which ingests publicly available data to provide updates to board-selected market KPIs (pipeline, households, chooser rate, public alternatives). The repo contains a ZCTA-localized data refresh pipeline, where outputs are keyed by `zcta5`, include `state_fips` and `county_fips`, and carry explicit `geo_method` metadata.

There are two primary inputs for this script:
1. **American Community Survey (ACS)**: The U.S. Census Bureau runs the ACS. It’s one of the Bureau’s core programs for “detailed characteristics” data (income, education, commuting, housing, etc.), collected continuously and released as 1-year and 5-year estimates.
2. **Zillow Market Data**: Zillow market data covers the home value index, rent index, median sale price, and housing inventory. By looking at housing market data, it provides a _more frequently refreshed_ input to the pipeline KPI, due to the annual+lagging nature of the ACS dataset.

Additional proxies including USPS, DC OSSE, and MDSE data are planned, to supplement further KPIs with more frequently refreshed data.

# II. Usage

## Command Line

1) Create a virtual environment and install deps:
```
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2) (Optional) Copy `.env.example` to `.env` and set API keys:
- `CENSUS_API_KEY`
- `DC_OPEN_DATA_APP_TOKEN` (optional for Socrata rate limits)

3) Configure your target ZIPs and dataset sources in `config/config.yaml`.

4) Run refresh:
```
python -m wesdash refresh --config config/config.yaml
```

Output workbook: `output/wes_board_dashboard.xlsx`

## Configuration

Configuration is set in `config.yaml`:
- **Geography**:
  - `geography.target_zips`: list of ZIPs to include (5-digit strings).
  - `geography.zip_to_zcta_overrides`: optional ZIP→ZCTA remaps for edge cases (keys/values must be 5-digit strings).
- **Project years**:
  - `project.start_year`: earliest year to pull (ACS pulls from this year forward).
  - `project.current_year`: optional override for the latest year; if empty, uses the current UTC year.
- **Paths**:
  - `paths.raw_dir`, `paths.processed_dir`, `paths.output_excel`, `paths.geo_cache_dir`: storage locations for raw, processed, workbook, and cached geospatial files. Do not change these values unless necessary.
- **Census API**:
  - `datasets.acs.api_key_env`: env var name for the Census API key (defaults to `CENSUS_API_KEY`).
- **DC Open Data (Socrata)**:
  - `datasets.dc_open_data.domain`: Socrata domain (default `data.dc.gov`).
  - `datasets.dc_open_data.datasets`: list of datasets with:
    - `dataset_id`: Socrata 4x4 ID (required; replace `REPLACE_ME`).
    - `name`: output file name (used for sheet naming).
    - `date_field`, `lat_field`, `lon_field`, `zip_field`, `value_field`: parsing controls.
    - `soql`: optional filter query.
- **Housing (Zillow ZIP)**:
  - `datasets.housing_zip.<metric>.local_path`: local CSV path (preferred for stability).
  - `datasets.housing_zip.<metric>.urls`: list of download URLs to try if `local_path` is empty.
- **USPS activity proxy**:
  - `datasets.usps_activity.source_url` or `local_path`: required to enable.
  - `tract_field`, `year_field`, `month_field`, `value_field`: map source columns.
- **OSSE / MSDE**:
  - `datasets.osse.source_url` or `local_path` (same for `msde_md`).
  - `sheet`: sheet name if Excel file has multiple tabs.
  - `zip_field` or `lat_field`/`lon_field`: geo columns for localization.
  - `rate_field`, `weight_field`, `year_field`: measure mapping.

Note: Datasets that are missing required configuration will be skipped with a warning (except ACS, which is required).

## What Gets Built

Current:
- `pipeline_acs5`: ACS 5-year pipeline counts (age 0-4, 5-9, 10-14)
- `pipeline_acs1`: ACS 1-year allocated pipeline counts (age 0-4, 5-9, 10-14)
- `pipeline_housing`: Zillow ZIP monthly series (prices, rents, inventory)
- `households`: ACS 5-year + ACS 1-year (allocated) household indicators
- `chooser`: ACS 5-year + ACS 1-year (allocated) enrollment/chooser indicators

Planned:
- `pipeline_usps`: USPS activity proxy (monthly)
- `pipeline_dc_open`: DC Open Data activity (monthly)
- `public_alternatives`: OSSE + MSDE school-level aggregated indicators
- `data_dictionary`: field definitions, geo method, and limitations

## Caching

Raw responses: `data/raw/<dataset>/<YYYY-MM-DD>/...`

Processed outputs: `data/processed/<dataset>/<YYYY-MM-DD>/<dataset>.parquet`

## Smoke Checks

The refresh command validates:
- all outputs have `zcta5`
- target ZCTAs are present
- `geo_method` is present and valid

# III. Source Reference

## Dataset Overview

- **ACS 5-year**: Native ZCTA via Census API. Stable estimates for small geographies.
- **ACS 1-year**: County-level values allocated to ZCTA using ACS 5-year population-weighted shares.
- **ACS measures used**:
  - `B01001` sex-by-age for child counts (0-4, 5-9, 10-14).
  - `S1101` households with own children under 18.
  - `B19131` high-income households with own children (labels matching $150k+).
  - `B14003` public vs private enrollment ages 3-14 (chooser rate).
- **Housing (Zillow ZIP)**: Monthly ZIP-level series (ZHVI, ZORI, median sale price, inventory).
- **USPS activity proxy**: Configure a tract-level vacancy/activity dataset and allocate to ZCTA.
- **DC Open Data (Socrata)**: Point/address records joined to ZCTA polygons.
- **OSSE/MSDE**: School-level data aggregated to ZIP/ZCTA (weighted if enrollment is provided).

## American Community Survey (ACS)
ACS data is gathered every year and compiled into both a 1-year and a rolling 5-year dataset. The rolling 5 year dataset gets recomputed every year as the window moves forward. At the 5 year, releases of ACS data lag. For example, Census’s 2024 release materials show 2020–2024 ACS 5-year estimates scheduled for December 11, 2025, and the ACS “updates” page notes the next release on January 29 will include the 2020–2024 5-year estimates. The 5 year is great for small geographies (tracts, ZCTAs) because it’s stable. For small geographies (like many ZCTAs or census tracts), the number of sampled households in any single year is often too small to produce stable estimates.

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
