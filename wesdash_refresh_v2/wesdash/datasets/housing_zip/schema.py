DATASET = {
    "name": "housing_zip",
    "source_name": "Zillow Research (ZIP)",
    "source_refresh_cadence": "monthly",
    "geo_method": "native_zip",
    "limitations": "Zillow ZIP series are modeled estimates and may revise historically; coverage varies by ZIP.",
    "measures": {
        "zhvi": "Zillow Home Value Index (all homes)",
        "zori": "Zillow Observed Rent Index",
        "median_sale_price": "Median sale price",
        "inventory": "Active listings inventory",
    },
}
