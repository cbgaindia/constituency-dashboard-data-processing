# About
These transformer are used to tranform the data for `https://constituency.openbudgetsindia.org/`. The data comes in scheme wise `xls` format and gets tranformed to state wise `csv` and `json`. For constituency dashboard, 2 files per state are created: `state-name_ac` and `state-name_pc`.

# Steps To Transform
- Place all the `xls` files in the `scheme-wise` folder
- Run the `transfomer.py` script, this will create a folder `state-wise` with all the csv's
- Check the csv's data in the `state-wise` folder, somtimes this data is broken in the following ways:
    - `NaN` value instead of empty string or 0
    - Long decimal point values for some indicators (This happens because original files have incorrect data)
    - Empty values as `-`. Replace them with empty string `""`
- Run the `json-transfomer.py` script, this will create a folder `state-wise-json` with all the json's
- Run the `metdata-tranformer.py` script, this will create a folder `metadata` withe the metadata json file
- Check the json metadata file, it has `utf-8` encoding characters which need to be removed manually and it sometimes has `NaN` values which need to changed to empty string
- Upload the data on CKAN to their respective organisation and datasets