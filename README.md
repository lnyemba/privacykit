# deid-risk

This project is intended to compute an estimated value of risk for a given database.

    1. Pull meta data of the database  and create a dataset via joins
    2. Generate the dataset with random selection of features
    3. Compute risk via SQL using group by
## Python environment

The following are the dependencies needed to run the code:

        pandas
        numpy
        pandas-gbq
        google-cloud-bigquery

        
## Usage

**Generate The merged dataset**

    python risk.py create --i_dataset <in dataset|schema> --o_dataset <out dataset|schema> --table <name> --path <bigquery-key-file>  --key <patient-id-field-name> [--file ]


**Compute risk (marketer, prosecutor)**

    python risk.py compute --i_dataset <dataset> --table <name> --path <bigquery-key-file>  --key <patient-id-field-name> 
## Limitations
    - It works against bigquery for now
    
    @TODO:    
        - Need to write a transport layer (database interface)
        - Support for referential integrity, so one table can be selected and a dataset derived given referential integrity
        - Add support for journalist risk