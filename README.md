## Configure

Use `.env` file or environment variables to set the parameters. The parameters are:

* **GRIB2_FILES_DIRECTORY_URL** (string) - the URL of the directory with all the GRIB2 files we want to process

For the very first local run you could copy-paste the example configuration:

```bash
cp windy_app_assessment/.env.TEMPLATE windy_app_assessment/.env
```

## Run

```bash
python windy_app_assessment/pull_grib2_files.py
```

## Improvements

1. Use `uvloop` for better performance
2. Dockerize and make it ready for K8s