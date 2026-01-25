DATASET = {
    "name": "usps_activity",
    "source_name": "HUD USPS Vacancy (tract proxy)",
    "source_refresh_cadence": "monthly",
    "geo_method": "boundary_to_zcta_weighted",
    "limitations": "USPS vacancy data are tract-level proxies allocated to ZCTAs by area weights; ZIP/ZCTA equivalence is not guaranteed.",
    "measures": {
        "active_address_count": "Active addresses (proxy for local activity)",
    },
}
