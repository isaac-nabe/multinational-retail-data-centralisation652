# Multinational Retail Data Centralisation

## Table of Contents

1. [Description](#description)
2. [Installation Instructions](#installation-instructions)
3. [Usage Instructions](#usage-instructions)
4. [File Structure](#file-structure)
5. [Lessons Learned](#lessons-learned)
6. [License](#license)

## Description

This project centralizes sales data for a multinational company into a single PostgreSQL database, making it easily accessible and analyzable. The primary tasks include setting up the database, extracting data from various sources (such as AWS RDS, PDF files, APIs, and S3), cleaning the data, and loading it into a local PostgreSQL database. This centralized database acts as a single source of truth for sales data, facilitating up-to-date metrics for the business.

### What I Learned:

- How to set up and connect to both remote and local PostgreSQL databases using SQLAlchemy.
- How to securely handle database credentials using YAML files.
- Extracting data from remote databases and loading it into Pandas DataFrames.
- Extracting data from PDFs using `tabula-py`.
- Extracting data from APIs and handling authentication.
- Extracting data from S3 buckets using `boto3`.
- Extracting data from JSON files using `requests`.
- Cleaning data, including handling missing columns and data types.
- Debugging and error handling in data processing.
- Using `.gitignore` to exclude sensitive files from version control.
- Solidified understanding of git staging, committing, and pushing processes.
- Importance of maintaining a robust data pipeline, breaking down code into single functions, and including debugging processes.
- Refactoring and optimizing code for better readability, maintainability, and performance.
- Using type annotations and docstrings to improve code clarity.

### Specific Lessons Learned - "How to build an ETL Pipeline"

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

7. **Code Refactoring and Optimization**:
   - Improved code readability and maintainability by using meaningful naming conventions, eliminating code duplication, and adhering to the Single Responsibility Principle (SRP).
   - Implemented proper error handling and logging to enhance code robustness.
   - Used type annotations and consistent docstrings to improve code clarity and documentation.

8. **SQL Query Optimization and Analysis**:
   - Learned the importance of decomposing complex SQL queries into manageable steps to ensure accuracy and efficiency.
   - Implemented window functions (like `LEAD`) to calculate differences between rows, simplifying complex calculations.
   - Utilized `EXTRACT(EPOCH FROM ...)` to handle time differences in a straightforward manner, converting seconds into human-readable formats.
   - Understood the importance of ordering and partitioning data correctly to ensure logical progression and accurate results.
   - Recognized the value of intermediate CTEs (Common Table Expressions) for breaking down and debugging SQL queries effectively.

9. **Foundations for Robust Data Pipelines**:
   - Emphasized setting up robust data cleaning and validation processes early in the project to avoid issues later.
   - Addressed issues such as handling 'nan' values by implementing validity filters, ensuring better data quality from the start.
   - Learned that thorough early-stage problem solving and data integrity checks save significant time and effort in later stages of the project.
   - Understand the logic of the operations you want to execute, this way you at least know what you want to do even if you're not certain of    how to yet.

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
3. Run the `Full_M3_Script.sql` to update data types and schema changes across multiple tasks:
    ```sh
    psql -h your_host -U your_username -d your_database -f Milestone_3/Full_M3_Script.sql
    ```
4.  Create a shell script file named "run_milestone_4_tasks.sh":
    ```
        echo '#!/bin/bash' > run_milestone_4_tasks.sh
        echo 'psql -h your_host -U your_username -d your_database -f Milestone_4/task_1.sql' >> run_milestone_4_tasks.sh
        echo 'psql -h your_host -U your_username -d your_database -f Milestone_4/task_2.sql' >> run_milestone_4_tasks.sh
        echo 'psql -h your_host -U your_username -d your_database -f Milestone_4/task_3.sql' >> run_milestone_4_tasks.sh
        echo 'psql -h your_host -U your_username -d your_database -f Milestone_4/task_4.sql' >> run_milestone_4_tasks.sh
        echo 'psql -h your_host -U your_username -d your_database -f Milestone_4/task_5.sql' >> run_milestone_4_tasks.sh
        echo 'psql -h your_host -U your_username -d your_database -f Milestone_4/task_6.sql' >> run_milestone_4_tasks.sh
        echo 'psql -h your_host -U your_username -d your_database -f Milestone_4/task_7.sql' >> run_milestone_4_tasks.sh
        echo 'psql -h your_host -U your_username -d your_database -f Milestone_4/task_8.sql' >> run_milestone_4_tasks.sh
        echo 'psql -h your_host -U your_username -d your_database -f Milestone_4/task_9.sql' >> run_milestone_4_tasks.sh
        chmod +x run_milestone_4_tasks.sh
    ```
    Run the shell script to execute all Milestone 4 task scripts:
    ```sh
        ./run_milestone_4_tasks.sh
    ```

## File Structure

```
multinational-retail-data-centralisation652/
├── Milestone_2/
│   ├── config.py
│   ├── data_cleaning.py
│   ├── data_extraction.py
│   ├── database_utils.py
│   ├── db_creds_local.yaml
│   ├── db_creds_rds.yaml
│   └── main.py
├── Milestone_3/
│   └── Full_M3_Script.sql
├── Milestone_4/
│   ├── task_1.sql
│   ├── task_2.sql
│   ├── task_3.sql
│   ├── task_4.sql
│   ├── task_5.sql
│   ├── task_6.sql
│   ├── task_7.sql
│   ├── task_8.sql
│   └── task_9.sql
├── .gitignore
├── README.md
└── required_packages.txt
```

### Description of Files

- **.gitignore**: Specifies files and directories to be ignored by git, including credentials and `__pycache__` directories.
- **config.py**: Contains the API key and other configuration variables.
- **db_creds_rds.yaml**: YAML file containing credentials for the remote RDS database (should be added to .gitignore).
- **db_creds_local.yaml**: YAML file containing credentials for the local PostgreSQL database (should be added to .gitignore).
- **required_packages.txt**: File containing the required Python packages for the project.
- **README.md**: This README file.

- **data_cleaning.py**: Contains the `DataCleaning` class for cleaning extracted data.
- **data_extraction.py**: Contains the `DataExtractor` class for extracting data from various sources.
- **database_utils.py**: Contains the `DatabaseConnector` class for handling database connections and operations.
- **main.py**: Main script to run the ETL (Extract, Transform, Load) process, integrating all modules.

- **Full_M3_Script.sql**: Consolidated and optimized SQL script for updating data types and schema changes across multiple tasks.

- **task_1.sql**: SQL script to find out how many stores the business has and in which countries.
- **task_2.sql**: SQL script to find out which locations currently have the most stores.
- **task_3.sql**: SQL script to determine which months produced the largest amount of sales.
- **task_4.sql**: SQL script to identify how many sales are coming from online.
- **task_5.sql**: SQL script to calculate the percentage of sales through each type of store.
- **task_6.sql**: SQL script to discover which month in each year had the highest cost of sales.
- **task_7.sql**: SQL script to get our staff headcount.
- **task_8.sql**: SQL script to see which type of store in Germany is selling the most.
- **task_9.sql**: SQL script to measure how quickly the company is making sales.


## License

This project is licensed under the MIT License. See the [LICENSE](https://choosealicense.com/licenses/mit/) file for more details.
