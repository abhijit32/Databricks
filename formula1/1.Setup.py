# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

storage_account_name = dbutils.secrets.get(scope="formula1-secret-scope", key="databricks-storage-account-name")
client_id = dbutils.secrets.get(scope="formula1-secret-scope", key="databricks-client-id")
tenant_id = dbutils.secrets.get(scope="formula1-secret-scope", key="databricks-tenant-id")
secret = dbutils.secrets.get(scope="formula1-secret-scope", key="databricks-secret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{client_id}",
          "fs.azure.account.oauth2.client.secret": f"{secret}",
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

container_name = "raw" 
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{container_name}",
  extra_configs = configs)

# COMMAND ----------

container_name = "processed" 
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{container_name}",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.ls("/mnt/raw")

# COMMAND ----------

dbutils.fs.ls("/mnt/processed")

# COMMAND ----------

dbutils.notebook.exit(f'success run {dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()}')

# COMMAND ----------


