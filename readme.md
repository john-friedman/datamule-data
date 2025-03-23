# datamule data

Contains data used for the [datamule](https://datamule.xyz/) project.

## data

### Filer Metadata

Information on every SEC Submissions Filer. For convenience, filers are seperated into listed & unlisted. Listed means has a ticker. Unlisted includes individuals.

Updated daily at 4:00 am ET. (2:00 AM ET is when the submissions.zip file is updated)

#### Metadata
* listed_filer_metadata.csv
* unlisted_filer_metadata.csv

columns: name, cik, business_city, business_stateOrCountry, business_stateOrCountryDescription, business_street1, business_street2, business_zipCode, category, description, ein, entityType, exchanges, fiscalYearEnd, insiderTransactionForIssuerExists, insiderTransactionForOwnerExists, mailing_city, mailing_stateOrCountry, mailing_stateOrCountryDescription, mailing_street1, mailing_street2, mailing_zipCode, ownerOrg, phone, sic, sicDescription, stateOfIncorporation, stateOfIncorporationDescription, tickers

#### Former Names
* listed_former_names.csv
* unlisted_former_names.csv

columns: cik, name, start_date, end_date

Current name's end date is left blank. If entity did not have a name change recorded during the period (approximately 1994-present) start date is set to entity's first submission.

Pipeline: downloads the [submissions zip](https://www.sec.gov/search-filings/edgar-application-programming-interfaces), then decompresses the first 2kb in each file within the zip, parsing the metadata, and finally uploads to github.


## indicators data

Updated nightly.