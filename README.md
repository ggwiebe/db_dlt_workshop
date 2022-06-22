# db_dlt_workshop
Databricks Delta Live Tables Workshop

This Repo has all the contents necessary to run a moderate (moderately advanced / complete) DLT workshop, from data discovery & profiling,
to simple pipeline developmemnt with Delta Live Tables (DLT) through to more advanced DLT with CDC, SCD and other event monitoring facilities.  

X. Worshop Flow
  
This workshop follows a flow that:
1. Manual Data Wrangling (upload data file, query & profile the data & structure); this serves as the starting point for:
2. Builds the two channels
3. Builds a multi-channel workflow
4. Defines a DLT EventLog table and creates associated queries
  
**Pipeline Development Note:** for each channel, you will find the code commented out back to the simplest state; comment & uncomment subequent sections of code to go from a simple pipeline to an enhanced pipeline.  
  
**Data Files Note:** The workshop can work with data files in a number of ways (e.g. DBFS and external cloud sources); The screenshots in the associated presentation were done from DBFS. DBFS has the benefit of being easy to rename the files to hide from the pipeline and exposing as needed. The approach folloed in this workshop presentation is:
- (Pre-workshop) Create a retail sandbox database for the first data wrangling elements (pre-DLT); e.g. ggw_retail_sandbox 
- (Optional Pre-workshop) Creating a separate file upload dbfs folder under /FileStore/tables, e.g. /FileStore/tables/ggw_retail_wshp; <-- Withouth this step the /FileStore/tables folder will be used for subequent "Create New Table" wizard and this may have many other files present.
- Begin workshop "1. Manual Data Wrangling" section by using the Data Explorer "Create New Table" tool and its "Upload File" option to upload the supplied channels.csv & subsequently the customers.csv; This step is to create a sandbox version of the table from the manually ingested file;
- "Upload" all remaining sample files to this same folder using the Data Explorer tool; *Note* this should match the CloudFiles location used in your first DB SQL queries and DLT pipeline notebooks)
- Rename all but the original files from .csv to .cs_ (or some other such extenstion that will not get automatically picked up);
- During DLT demonstration, rename the individual files to .csv to trigger the ingestion of these files and associated data scenarios (expectation failure handling, incremental changes, etc.)
Here is an example of the "Create New Table" wizard:
<img src="https://raw.githubusercontent.com/ggwiebe/db_dlt_workshop/main/images/CreateNewTable_UploadFile.png" width=600>
  
A. Sales Channels Reference Data
- From Data Profiling and simple pipeline development with DLT through to a data and tables lineage tracking
  
The following screenshot shows the enhanced / final version that includes SCD Type tracking and data lineage:
<img src="https://raw.githubusercontent.com/ggwiebe/db_dlt_workshop/main/images/DLT_ChannelReference_Pipeline.png" width=1024>
<!-- ![DLT Channel Reference Pipeline](https://raw.githubusercontent.com/ggwiebe/db_dlt_workshop/main/images/DLT_ChannelReference_Pipeline.png) -->
  
B. Customers Master Data
- A classic MDM application for Customer data
- Leveraging SCD Type 2
- Views and Joins to enrich pipelines
- Change Data Capture (CDC) pattern delivery with APPLY INTO DLT syntax 
- Improve data quality
- Gold Serving data mart
  
The following screenshot shows the enhanced / final version that includes bronze -> silver -> gold data landscape with views, expectations, cdc, and SCD Type 2 tracking:
<img src="https://raw.githubusercontent.com/ggwiebe/db_dlt_workshop/main/images/DLT_CustomerMaster_Pipeline.png" width=1024>
<!-- ![DLT Customer Master Pipeline](https://raw.githubusercontent.com/ggwiebe/db_dlt_workshop/main/images/DLT_CustomerMaster_Pipeline.png) -->
  
C. DLT Event Monitoring
- Queries for pipelines, lineage, and runtime metrics
- Event Dashboards
  
The following screenshot shows a sample DLT monitoring dashboard based on the DLT event log table and event, lineage and metric queries:
<img src="https://raw.githubusercontent.com/ggwiebe/db_dlt_workshop/main/images/RetailSales_DLT_MonitoringDashboard.png" width=1024>
<!-- ![DLT Monitoring](https://raw.githubusercontent.com/ggwiebe/db_dlt_workshop/main/images/RetailSales_DLT_MonitoringDashboard.png) -->
  
D. DLT and Databricks Workflows
- Support multi-step data pipelines
- Enable componentized development
- Enable precendence dependencies (e.g. Product Master Load in parallel with Customer Master Load, both as precedents for SalesOrder)
   
The following screenshot shows a multi-step workflow that runs the Sales Channel reference data load, on which the Customer data load is dependent on (i.e. A customer's Sales Channel should use the most up-to-date definition of the SalesChannel info):
<img src="https://raw.githubusercontent.com/ggwiebe/db_dlt_workshop/main/images/Job_Channel_Customer_workflow.png" width=800>
<!-- ![DLT Process Flow](https://raw.githubusercontent.com/ggwiebe/db_dlt_workshop/main/images/Job_Channel_Customer_workflow.png) -->
  
