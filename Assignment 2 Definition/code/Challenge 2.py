# Databricks notebook source
msdl_account_key = dbutils.secrets.get(scope = 'ms-adls-scope', key = 'ms-adls-account-key')

# COMMAND ----------

spark.conf.set("fs.azure.account.key.rmsandboxst.dfs.core.windows.net",msdl_account_key)

# COMMAND ----------

SourceFilePath = 'abfss://raw@rmsandboxst.dfs.core.windows.net/marketing/'
SourceFeedName = 'marketingdataset.csv'
output_path_1 = "abfss://raw@rmsandboxst.dfs.core.windows.net/marketing/balance_account_daily"
output_path_2 = "abfss://raw@rmsandboxst.dfs.core.windows.net/marketing/balance_account_summary"
SourceFile = SourceFilePath + SourceFeedName
print(SourceFile)

# COMMAND ----------

dfFeed = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(SourceFile)  

# COMMAND ----------

dfFeed.createOrReplaceTempView("msrawdata")

# COMMAND ----------

# Execute the SQL query and save it as a DataFrame
dfBalance_1 = spark.sql("""
SELECT 
    TransactionDate,
    AccountNumber,
    TransactionType,
    CASE 
        WHEN TRIM(TransactionType) = 'Credit' THEN Amount
        WHEN TRIM(TransactionType) = 'Debit' THEN -Amount
        ELSE 0
    END AS AdjustedAmount,
    SUM(
        CASE 
            WHEN TRIM(TransactionType) = 'Credit' THEN Amount
            WHEN TRIM(TransactionType) = 'Debit' THEN -Amount
            ELSE 0
        END
    ) OVER (
        PARTITION BY AccountNumber 
        ORDER BY TransactionDate 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS Balance
FROM 
    msrawdata
ORDER BY 
    AccountNumber, TransactionDate
""")

# COMMAND ----------

dfBalance_1.createOrReplaceTempView("balance_daily")

# COMMAND ----------

dfBalance_2 = spark.sql("""
Select accountNumber, SUM(AdjustedAmount) as totalBalance
From balance_daily 
Group by accountNumber
Order by 1
""")


# COMMAND ----------

dfBalance_1.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path_1)

# COMMAND ----------

dfBalance_2.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path_2)