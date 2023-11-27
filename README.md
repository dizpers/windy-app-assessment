## Configure

Use `.env` file or environment variables to set the parameters. The parameters are:

* **GRIB2_FILES_DIRECTORY_URL** (string) - the URL of the directory with all the GRIB2 files we want to process

For the very first local run you could copy-paste the example configuration:

```bash
cp windy_app_assessment/.env.TEMPLATE windy_app_assessment/.env
```
## Prepare

1. Install `eccodes` package (MacOS) or `libeccodes-dev` package (Linux)
2. Install dependencies `poetry install`
3. Activate environment `poetry shell`

## Run

```bash
python windy_app_assessment/pull_grib2_files.py
```

## Improve

1. Use `uvloop` for better performance
2. Dockerize and make it ready for K8s
3. See if it'd be better to use in-memory files (less time for disk i/o operations)
4. Currently, it expects the list of `.grib2.bz2` files on the "directory" page. Other data providers might follow another way. So we should make this script more flexible to support other rules of extraction.
5. There's a chance that we already did all the calculations, so no need to process some files
6. Add error handling
7. Add logging
8. Add tests
9. Address other TODOs in the code