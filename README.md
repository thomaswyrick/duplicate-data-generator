# duplicate-data-generator
This script will create duplicates in known quantities to test record linkage (e.g. MDM) systems.  To further test the fuzzy matching of your record linkage system, the script can trasposition and mistype duplicated columns.  Please note, this script is only a thin wrapper over the existing [Faker](https://github.com/joke2k/faker) library and would be nothing without it.

## Installation
1. Download the repository
2. Install the referenced modules with: pip install -r requirements.txt

## Usage
python duplicate_data_generator.py --config dup_data_config.json --output output.xlsx

## Config File Settings
### total_row_cnt
The total number of rows you would like produce.
### duplication_rate
The known duplication rate of the records to produce.
### localization
See [here](https://faker.readthedocs.io/en/master/locales.html) for a list of possible values
### columns
#### name
The name of the column written to the Excel sheet
#### type
Supported types are: "first_name","last_name", "street_address", "secondary_address", "city", "state", "postcode", "current_country", "phone_number", "email"
#### transposition_chars
The number of characeters to transpotion (switch) within the the cell of the column
#### mistype_chars
The number of characeters to mistype (replace one character for another) within the the cell of the column


