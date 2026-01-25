DATASET = {
    "name": "dc_open_data",
    "source_name": "DC Open Data (Socrata)",
    "source_refresh_cadence": "weekly",
    "geo_method": "point_to_zcta_join",
    "limitations": "Point records are assigned to ZCTAs via spatial join; records with missing coordinates are dropped.",
    "measures": {
        "record_count": "Count of records per month",
    },
}
