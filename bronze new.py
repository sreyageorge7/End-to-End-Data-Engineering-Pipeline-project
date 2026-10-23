# Databricks notebook source
storage_account = "sreyaastorage"
container="bronze"

client_id = dbutils.secrets.get(
    scope="sreyascope",
    key="clientid"
)

client_secret = dbutils.secrets.get(
    scope="sreyascope",
    key="clientsecretvalue"
)

tenant_id = dbutils.secrets.get(
    scope="sreyascope",
    key="tenantsecretid"
)

host = f"{storage_account}.dfs.core.windows.net"

spark.conf.set(
    f"fs.azure.account.auth.type.{host}",
    "OAuth"
)

spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{host}",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.id.{host}",
    client_id
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.secret.{host}",
    client_secret
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{host}",
    f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
)

# COMMAND ----------


# Check whether Databricks can access the Bronze container

bronze_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/"  # Verify that the cluster's managed identity or service principal has the correct permissions on the storage account.  # Ensure container and storage_account are correctly set and have proper permissions, and that the cluster has the correct access configuration.

display(dbutils.fs.ls(bronze_path))  # Check if the credentials and permissions are correctly configured.


# COMMAND ----------

storage_account = "sreyaastorage"
container = "bronze"

bronze_path = (
    f"abfss://{container}@{storage_account}.dfs.core.windows.net/"
)

display(dbutils.fs.ls(bronze_path))

# COMMAND ----------

display(
    dbutils.fs.ls(
        f"{bronze_path}transactions/"
    )
)

# COMMAND ----------


# Read the Bronze datasets

transactions_df = spark.read.parquet(
    f"{bronze_path}transactions/"
)

products_df = spark.read.parquet(
    f"{bronze_path}products/"
)

stores_df = spark.read.parquet(
    f"{bronze_path}stores/"
)
customer_file = f"{bronze_path}customers/customers.json"

customers_df = (
    spark.read
    .option("multiLine", True)
    .json(customer_file)
)

# Check the data
print("Transactions:", transactions_df.count())
print("Products:", products_df.count())
print("Stores:", stores_df.count())
print("Customers:", customers_df.count())

display(transactions_df)
display(products_df)
display(stores_df)
display(customers_df)


# COMMAND ----------

