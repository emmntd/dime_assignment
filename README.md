# dime_assignment
## **Dime Data Engineer Assignment - Nattadol Prechavibul**

**The take-home assignment requires you to:**
- Get the data from a free financial data API called FMP (https://site.financialmodelingprep.com/developer/docs/)
Historical Dividends (https://site.financialmodelingprep.com/developer/docs/#Historical-Dividends)
- Delisted companies ((https://site.financialmodelingprep.com/developer/docs/delisted-companies-api/))
- Store the retrieved data in any database you are comfortable working with (for example, you may setup MySQL on your local machine)
- Note that, we prefer you to spend some time parsing HTTP request/response rather than FMP library to measure your skill.

_________________________________________________________________________________________________

## For the assignment according to my repository,
![image](https://user-images.githubusercontent.com/86764643/192811840-62a4ad08-d608-461d-a0b8-c2c93129cfe6.png)

The main script is **get_fmp_data.py** which do all the processes e.g. extract, transform and load into PostgresDB by calling functions from itself and from **utils.py** as plugins

Also I created **secret.pickle** (I did not put into this repo) to store my API_KEY and postgres credentials (host, user , password) which can be replaced with AWS Secret Manager

## **Getting Data Part**
After I got the data and converted into DataFrame plus some data type transformations and added the column 'updated_time' as current utc timestamp to be used for deduplication and update data on the table. For the next step, I saved the data as .parquet file into path **'./data/{table}/{year}/{month}/{day}/'**
which I referred this as my **Data Lake** also give it the subpath with year, month, day in case that we can use these as Partitioning columns.

***The year, month, day are come from the script running date or it can be replaced with execution_date that pass from the scheduler.***

## **Create Table on PostgresDB**
Before create table, I need to prepare the table schema with **CREATE TABLE** script that saved in path **./sql/create_table/**, in case there's some column with specific column type e.g. date type or timestamp and then create if not exists (If there's a schema change I chose to drop it manually first for safer and re-create the table with the pipeline)

![image](https://user-images.githubusercontent.com/86764643/192816570-e4c341da-36f9-471c-84d0-e3a904f8524d.png)

And use **sqlalchemy** to create my Postgres connection with my credentials in secret. **(This function is in utils.py)**

![image](https://user-images.githubusercontent.com/86764643/192819501-6e2f030f-5527-43e8-b7d7-32fd970be9cb.png)

![image](https://user-images.githubusercontent.com/86764643/192821041-30ba0b3f-5b45-4d06-b161-ed23b210bea1.png)


## **Ingest Data into PostgresDB**
I use glob to fetch all the .parquet files from the folder of each table from each ingested date and read each parquet as dataframe.
After that I used df.to_sql method to insert data to the table.

![image](https://user-images.githubusercontent.com/86764643/192820740-a954f757-040c-4ad6-b089-fcdcf038e516.png)

## **Deduplicate Data by getting latest updated_time with its primary keys**
I had defined each primary key of each table on **tables_and_pks** dict and then pass into dedup query by using Postgres ctid and updated_time to help me get the latest record of each data with its primary keys.

![image](https://user-images.githubusercontent.com/86764643/192821812-a3fba1a4-3461-456d-9c87-c6706cfbc7ac.png)


**Finally Data is ingested into PostgresDB schema "fmp" with the latest updated_time**

**Table : fmp.historical_dividends**

![image](https://user-images.githubusercontent.com/86764643/192805901-0d01b469-870f-4492-af91-a565dd920cda.png)

**Table: fmp.delisted_companies**

![image](https://user-images.githubusercontent.com/86764643/192807289-decc25cb-a6d2-4cb5-a07b-a71a0324260a.png)

## **Thank you**
