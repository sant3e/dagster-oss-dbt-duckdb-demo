# Landing data

Drop daily snapshot CSVs into `./landing/` using the naming convention:

```
sales_details_YYYY_MM_DD.csv
cust_info_YYYY_MM_DD.csv
loc_a101_YYYY_MM_DD.csv
```

Each daily file must include a `snapshot_date` column whose value equals the date in the filename.

The `landing_file_sensor` in `elt_pipelines` polls this folder every 30 seconds and launches the corresponding partitioned landing asset whenever a new file appears.

Static reference seeds (`prd_info`, `CUST_AZ12`, `PX_CAT_G1V2`) are loaded via `make seed` and do not need to be placed here.
