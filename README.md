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
3. See if it'd be better to use in-memory files (less time for disk i/o operations)
4. Currently, it expects the list of `.grib2.bz2` files on the "directory" page. Other data providers might follow another way. So we should make this script more flexible to support other rules of extraction.
5. Add error handling
6. Add logging
7. Add tests