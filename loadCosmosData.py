#!/usr/bin/env python
# coding: utf-8

#install library

# ## loadCosmosData
# 
# 
# 

# In[ ]:


cosmosEndpoint = "https://myprojectcosmosdb.documents.azure.com:443/"
cosmosMasterKey = "FhBqQBPYXWhzaIyDtDPx39ydulDsR4Hcpw5HgnaBHPGYJaiOUwLXzFztVgQZHbUM35HsRvXayJmYACDbq881Vg=="
cosmosDatabaseName = "mycosmosdb"
cosmosContainerName = "basicuser"

cfg = {
  "spark.cosmos.accountEndpoint" : cosmosEndpoint,
  "spark.cosmos.accountKey" : cosmosMasterKey,
  "spark.cosmos.database" : cosmosDatabaseName,
  "spark.cosmos.container" : cosmosContainerName,
}


# In[ ]:


# Configure Catalog Api to be used
spark.conf.set("spark.sql.catalog.cosmosCatalog", "com.azure.cosmos.spark.CosmosCatalog")
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountEndpoint", cosmosEndpoint)
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountKey", cosmosMasterKey)


# In[ ]:


df = spark.read.format("cosmos.oltp").options(**cfg).option("spark.cosmos.read.inferSchema.enabled", "true").load()


# In[ ]:


from cryptography.fernet import Fernet
# Put this somewhere safe!
key = Fernet.generate_key()
f = Fernet(key)
token = f.encrypt(b"A really secret message. Not for prying eyes.")

# Define Encrypt User Defined Function 
def encrypt_val(clear_text,MASTER_KEY):
    from cryptography.fernet import Fernet
    f = Fernet(MASTER_KEY)
    clear_text_b=bytes(clear_text, 'utf-8')
    cipher_text = f.encrypt(clear_text_b)
    cipher_text = str(cipher_text.decode('ascii'))
    return cipher_text

from pyspark.sql.functions import udf, lit, md5
from pyspark.sql.types import StringType
# from pyspark.dbutils import DBUtils
# dbutils = DBUtils(spark)

 
# Register UDF's
encrypt = udf(encrypt_val, StringType())

key = Fernet.generate_key()
f = Fernet(key)
 
# Fetch key from secrets
# encryptionKey = dbutils.preview.secret.get(scope = "encrypt", key = "fernetkey")
 
# Encrypt the data 
#df = spark.table("Test_Encryption")
#encrypted = df.withColumn("birthday", encrypt("birthday",lit(encryptionKey)))
encrypted = df.withColumn("birthday", encrypt("birthday",lit(key)))


# In[ ]:


encrypted.write.format("cosmos.oltp").options(**cfg).mode("Append").save()

