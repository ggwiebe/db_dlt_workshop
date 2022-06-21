# db_dlt_workshop
Databricks Delta Live Tables Workshop

This Repo has all the contents necessary to run a moderate (moderately advanced / complete) DLT workshop, from data discovery & profiling,
to simple pipeline developmemnt with Delta Live Tables (DLT) through to more advanced DLT with CDC, SCD and other event monitoring facilities.
  
A. Sales Channels Reference Data
- From Data Profiling and simple pipeline development with DLT through to a data and tables lineage tracking
  
![DLT Process Flow](https://raw.githubusercontent.com/ggwiebe/db_dlt_workshop/main/images/DLT_ChannelReference_Pipeline.png)
  
B. Customers Master Data
- A classic MDM application for Customer data
- Leveraging SCD Type 2
- Views and Joins to enrich pipelines
- Change Data Capture (CDC) pattern delivery with APPLY INTO DLT syntax 
- Improve data quality
- Gold Serving data mart
  
![DLT Process Flow](https://raw.githubusercontent.com/ggwiebe/db_dlt_workshop/main/images/DLT_CustomerMaster_Pipeline.png)
  
C. DLT Event Monitoring
- Queries for pipelines, lineage, and runtime metrics
- Event Dashboards
  
![DLT Process Flow](https://raw.githubusercontent.com/ggwiebe/db_dlt_workshop/main/images/RetailSales_DLT_MonitoringDashboard.png)
  
D. DLT and Databricks Workflows
- Support multi-step data pipelines
- Enable componentized development
- Enable precendence dependencies (e.g. Product Master Load in parallel with Customer Master Load, both as precedents for SalesOrder)
   
![DLT Process Flow](https://raw.githubusercontent.com/ggwiebe/db_dlt_workshop/main/images/Job_Channel_Customer_workflow.png)
  
