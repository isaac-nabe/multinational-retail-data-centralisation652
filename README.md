# Multinational Retail Data Centralisation

## Table of Contents

1. [Description](#description)
2. [Installation Instructions](#installation-instructions)
3. [Usage Instructions](#usage-instructions)
4. [File Structure](#file-structure)
5. [Lessons Learned](#lessons-learned)
6. [License](#license)

## Description

This project centralizes sales data for a multinational company into a single PostgreSQL database, making it easily accessible and analyzable. The primary tasks include setting up the database, extracting data from various sources (such as AWS RDS and PDF files), cleaning the data, and loading it into a local PostgreSQL database. This centralized database acts as a single source of truth for sales data, facilitating up-to-date metrics for the business.

### What I Learned

- How to set up and connect to both remote and local PostgreSQL databases using SQLAlchemy.
- How to securely handle database credentials using YAML files.
- Extracting data from remote databases and loading it into Pandas DataFrames.
- Extracting data from PDFs using `tabula-py`.
- Cleaning data, including handling missing columns and data types.
- Debugging and error handling in data processing.
- Using `.gitignore` to exclude sensitive files from version control.
- Solidified understanding of git staging, committing, and pushing processes.
- Importance of maintaining a robust data pipeline, breaking down code into single functions, and including debugging processes.

## Installation Instructions

1. Ensure you have Python and PostgreSQL installed on your system.
2. Clone this repository to your local machine using:
    ```sh
    git clone https://github.com/isaac-nabe/multinational-retail-data-centralisation652.git
    ```
3. Navigate to the project directory:
    ```sh
    cd multinational-retail-data-centralisation652
    ```
4. Install the required Python packages:
    ```sh
    pip install -r required_packages.txt
    ```
5. Ensure you have two YAML files for database credentials:
    - `db_creds_rds.yaml` for the remote RDS database credentials.
    - `db_creds_local.yaml` for the local PostgreSQL database credentials.
6. Create a `config.py` file for your API key and other configuration variables:
    ```python
    pdf_link = 'your_pdf_link_here'
    API_KEY = 'your_api_key_here'
    number_of_stores_url = 'your_number_of_stores_url_here'
    store_url_template = 'your_store_url_template_here'
    s3_products_address = 'your_s3_products_address_here'
    s3_sale_dates_address = 'your_s3_sale_dates_address_here'
    ```
7. Add these files to your `.gitignore` to prevent them from being tracked in version control:
    ```plaintext
    db_creds_rds.yaml
    db_creds_local.yaml
    config.py
    __pycache__/
    ```

## Usage Instructions

1. Ensure your PostgreSQL service is running and accessible.
2. Run the main script to extract, clean, and load data into the local database:
    ```sh
    python main.py
    ```
3. Verify the data by checking the `dim_users`, `dim_card_details`, `dim_store_details`, `dim_products`, and `dim_date_times` tables in your `sales_data` database using pgAdmin4 or any SQL client:
    ```sql
    SELECT * FROM dim_users;
    SELECT * FROM dim_card_details;
    SELECT * FROM dim_store_details;
    SELECT * FROM dim_products;
    SELECT * FROM dim_date_times;
    ```

## File Structure
```
Multinational-Retail-Data-Centralisation/
├── .gitignore/
	├── config.py # Should be added to .gitignore
	├── db_creds_local.yaml # Should be added to .gitignore
	├── db_creds_rds.yaml # Should be added to .gitignore
├── data_cleaning.py
├── data_extraction.py
├── database_utils.py
├── main.py
├── README.md # This README.md
├── required_packages.txt
```

### Description of Files

- **.gitignore**: Specifies files and directories to be ignored by git, including credentials and `__pycache__` directories.
- **config.py**: Contains the API key and other configuration variables.
- **db_creds_rds.yaml**: YAML file containing credentials for the remote RDS database (should be added to .gitignore).
- **db_creds_local.yaml**: YAML file containing credentials for the local PostgreSQL database (should be added to .gitignore).
- **data_cleaning.py**: Contains the `DataCleaning` class for cleaning extracted data.
- **data_extraction.py**: Contains the `DataExtractor` class for extracting data from various sources.
- **database_utils.py**: Contains the `DatabaseConnector` class for handling database connections and operations.
- **main.py**: Main script to run the ETL (Extract, Transform, Load) process, integrating all modules.
- **README.md**: This README file.
- **required_packages.txt**: File containing the required Python packages for the project.

## Lessons Learned - "How to build an ETL Pipeline"

1. **Database Connectivity**:
   - Learned how to set up and connect to both remote and local PostgreSQL databases using SQLAlchemy.

2. **Handling YAML Files**:
   - Gained experience in securely handling database credentials using YAML files and ensuring they are not tracked by version control.

3. **Data Extraction**:
   - Learned how to extract data from a remote database and load it into a Pandas DataFrame for further processing.
   - Learned how to extract data from a PDF using `tabula-py` and load it into a Pandas DataFrame.
   - Learned how to extract data from an API and handle authentication securely.
   - Learned how to extract data from an S3 bucket using `boto3`.
   - Learned how to extract data from a JSON file using `requests`.

4. **Data Cleaning**:
   - Understood the importance of handling missing columns, data types, and erroneous values during the data cleaning process.
   - Developed methods to clean user data, card data, store data, product data, and date events data effectively.

5. **Debugging and Error Handling**:
   - Gained experience in identifying and fixing various errors related to file handling, database connectivity, and data processing.

6. **Version Control Best Practices**:
   - Learned to use `.gitignore` to exclude sensitive files from version control and ensure a clean project repository.

## License

This project is licensed under the MIT License. See the [LICENSE](https://choosealicense.com/licenses/mit/) file for more details.
