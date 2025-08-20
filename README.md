# Airline-Analytics-Pipeline
<img width="976" height="388" alt="image" src="https://github.com/user-attachments/assets/b6f29229-ba3b-450e-95bb-2d4da80642be" />

1. **Create Azure Resources**
    - Set up a Resource Group in Azure to organize all project resources.
<img width="1918" height="856" alt="image" src="https://github.com/user-attachments/assets/5bd4e9c3-61b7-404e-81b0-aeeee934194a" />

2. **Set Up Integration Runtime in ADF**
    - Since the source data is on-premise, configure a Self-Hosted Integration Runtime (IR) in Azure Data Factory.
    - This allows ADF to securely access local files on your machine.
    - Register the IR and verify connectivity.

3. **Configure Linked Services**
    - Create linked services in ADF to connect sources and destinations:
      - ls_datalake → Azure Data Lake Storage Gen2
      - ls_onprem_file → On-premise files via Self-Hosted IR
      - ls_azuresql → SQL database
      - ls_databricks → Databricks workspace
    These act as bridges between source and destination.

4. **Create Parameterized Datasets**
      - Define datasets in ADF for both source files and destination parquet files.
      - Use parameters (e.g., p_file_name) to make the pipeline dynamic, allowing ingestion of multiple files without creating a separate dataset for each file.

**Datasets used in this project:** 
    
   - ds_emptyjson → points to empty.json in the data lake (JSON)
   - ds_lookup → points to last_load.json in the data lake (JSON)
   - ds_onprem_sink → Bronze folder in ADLS (Parquet)
   - ds_onprem_src → On-prem folder (CSV)
   - ds_sql → SQL folder in ADLS (Parquet)
   - ds_sql_source → Source SQL database (SQL DB)

5. Build Parameterized Pipeline

**Pipeline 1: Ingest On-Prem CSV to Bronze & Delta Tables**
- Create a pipeline with a ForEach activity to loop through all source CSV files.
- Inside the loop:
  - Use Copy Data activity to ingest each file into the Bronze layer in Azure Data Lake.
  - Use a Databricks notebook activity to transform the ingested Parquet files into Delta tables registered under the Databricks Unity Catalog.

**Pipeline 2: Incremental Load from SQL Database**
- Use Lookup activities to determine the data range for incremental loading:
  - Last Load Lookup: Read the last_load.json file in the data lake to get the last processed date.
  - Latest Load Lookup: Query the source SQL database (FactBookings) to find the latest booking_date.
- Use Copy Data activity to extract only new or updated records:

    SELECT * FROM dbo.FactBookings WHERE booking_date > '@{activity('LastLoad').output.firstRow.lastload}'AND booking_date <= '@{activity('LatestLoad').output.firstRow.latestload}'

  - Load the extracted records into a Parquet file in the Bronze layer.
- Update the last_load.json file in the data lake to reflect the latest processed date, ensuring the next run only ingests new data.

**Pipeline 3: Silver Layer Transformation with Switch Case**
- Create a parameterized pipeline with parameters:
  - table_name → specifies which table to process (e.g., booking, flight, airline).
  - batch_date → defines the batch of data to process.
- Add a Switch activity that evaluates table_name to determine which transformation logic to execute.
  - Each case queries the corresponding Bronze table in Databricks Unity Catalog (e.g., dev.bronze.airline).
  - Databricks notebooks transform the queried Bronze data into Silver Delta tables, partitioned by batch_date for optimized query performance.
 
**Pipeline 4: Gold Layer Transformation with Switch Case**
- Create a parameterized pipeline with parameters:
  - table_name → specifies which table to process (e.g., booking, flight, airline).
  - batch_date → defines the batch of data to process.
- Add a Switch activity that evaluates table_name to determine which transformation logic to execute.
  - Each case queries the corresponding Silver Delta table in Databricks Unity Catalog.
  - Databricks notebooks transform Silver data into Gold Delta tables, also partitioned by batch_date, optimized for analytics and Power BI reporting.

6. **Power BI Visualization**
    - Connect Power BI directly to Databricks Gold Delta tables in Unity Catalog.
    - Use the Databricks connector in Power BI Desktop to query Gold tables efficiently.
    - Built interactive report pages with KPIs and visual analytics, providing insights into:
      - Bookings
      - Passengers
      - Flight operations
    - Leveraged Gold tables partitioned by batch_date to improve query performance and report refresh times.

### Resources
- Full Medium article: [From On-Prem CSV to Cloud Analytics]()
- Sample notebooks, ADF JSONs, and Power BI files are all included in this repository.
