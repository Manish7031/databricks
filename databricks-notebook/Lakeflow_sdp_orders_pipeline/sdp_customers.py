from pyspark import pipelines as dp

@dp.materialized_view(
    comment="This is a materialized view of customer table"
)
def customer_bronze():
    df = spark.read.table("devenv.bronze.customer_raw")
    return df

@dp.view(
    comment="customer bronze temporary view"
)
def customers_bronze_vw():
    df = spark.readStream.table("customer_bronze")
    return df

# create customer SCD2
dp.create_streaming_table("customer_scd2_bronze")

# SCD2 customer
dp.create_auto_cdc_flow(
    target="customer_scd2_bronze",
    source="customers_bronze_vw",  # reference function, not string
    keys=["c_custkey"],
    stored_as_scd_type= 2,
    #except_column_list=["_src_action", "_src_insert_dt"],
    sequence_by="c_custkey"
)