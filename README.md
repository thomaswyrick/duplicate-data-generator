# duplicate-data-generator
This script will create duplicates in known quantities to test record linkage (e.g. MDM) systems.  To further test the fuzzy matching of your record linkage system, the script can trasposition and mistype duplicated columns.  

Note: this script is only a thin wrapper over the existing [Faker](https://github.com/joke2k/faker) library.

## Installation
1. Install Python 3+
2. Download the repository
3. Install the referenced modules with: pip install -r requirements.txt

## Usage Example
python duplicate_data_generator.py --column_file sample_column_files/en_US_columns.json --localization en_US --output out_US.csv --rows 1000000 --duprate .10

### Arguments
  --help show this help message and exit
  --column_file column configuration file
  --output_name output file name
  --rows the total number of rows to generate
  --duprate duplication rate
  --localization localization
  --cpus the number of cpus to use for mulitprocessing
  --batchsize the size of each batch to process

## Command Line Parameters
### column_file
The file path to json column configuration file
### output_name
The csv output file name
### rows
The total number of rows you would like produce
### duprate
The known duplication rate of the records to produce
### localization
See [here](https://faker.readthedocs.io/en/master/locales.html) for a list of possible values

## Column Configuration File Settings
The config json file takes an array of Column, which can be defined as:
### name (Required)
The name of the column in the generated csv file.
### type (Required)
The type of the column.  Supported types are:

first_name, last_name, street_address, secondary_address, city, state, postcode, country_code, phone_number, email, gender, date_of_birth
### fill_rate (Not Required)
The percentage of rows to populate for the given column.
### transposition_chars (Not Required)
The number of characters to transposition (e.g. switch around).
### mistype_chars (Not Required)
The number of characters to mistype.  Please note, this just assigns a random key from the keyboard.



