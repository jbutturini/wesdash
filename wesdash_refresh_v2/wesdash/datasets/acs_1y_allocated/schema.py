DATASET = {
    "name": "acs_1y_allocated",
    "source_name": "Census ACS 1-year (county, allocated to ZCTA)",
    "source_refresh_cadence": "annual",
    "geo_method": "county_to_zcta_weighted",
    "limitations": "Allocated from county-level ACS 1-year using ACS 5-year population weights; assumes uniform distribution within county.",
    "measures": {
        "population_total": "Total population (allocated)",
        "age0_4": "Population age 0-4 (allocated)",
        "age5_9": "Population age 5-9 (allocated)",
        "age10_14": "Population age 10-14 (allocated)",
        "hh_own_children_u18": "Households with own children under 18 (allocated)",
        "hhkids_income_150_plus": "Households with own children under 18 and income >=150k (allocated)",
        "hhkids_income_200_plus": "Households with own children under 18 and income >=200k (allocated)",
        "public_enrolled_3_14": "Public school enrollment ages 3-14 (allocated)",
        "private_enrolled_3_14": "Private school enrollment ages 3-14 (allocated)",
        "private_chooser_rate_3_14": "Private chooser rate ages 3-14 (allocated)",
    },
}
