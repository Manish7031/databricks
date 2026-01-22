from pyspark import pipelines as dp

@dp.table(
    table_properties = {"quality": "bronze"},
    comment = "This is a order bronze table"
)

def orders_bronze():
    df =  spark.readStream.table("devenv.bronze.orders_raw")
    return df

@dp.materialized_view(
    comment = "join view"
)

def orders_customer_joined():
    df_c = spark.read.table("customer_scd2_bronze").where("__END_AT is NULL")
    df_o = spark.read.table("orders_bronze")
    dfjoin = df_o.join(
    df_c,
    df_o.o_custkey == df_c.c_custkey,
    "left_outer"
)
    return dfjoin

