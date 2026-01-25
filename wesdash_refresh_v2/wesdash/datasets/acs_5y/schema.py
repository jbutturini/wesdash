DATASET = {
    "name": "acs_5y",
    "source_name": "Census ACS 5-year",
    "source_refresh_cadence": "annual",
    "geo_method": "native_zcta",
    "limitations": "ACS 5-year estimates are rolling averages; small-area estimates can lag current conditions.",
    "measures": {
        "population_total": "Total population (B01001_001E)",
        "age0_4": "Population age 0-4 (male+female)",
        "age5_9": "Population age 5-9 (male+female)",
        "age10_14": "Population age 10-14 (male+female)",
        "hh_own_children_u18": "Households with own children under 18",
        "hhkids_income_150_plus": "Households with own children under 18 and income >=150k",
        "hhkids_income_200_plus": "Households with own children under 18 and income >=200k",
        "public_enrolled_3_14": "Public school enrollment ages 3-14",
        "private_enrolled_3_14": "Private school enrollment ages 3-14",
        "private_chooser_rate_3_14": "Private chooser rate ages 3-14",
    },
}
