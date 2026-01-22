from pyspark import pipelines as dp

dp.create_sink(
    name="delta_sink",
    format="delta",
    options={"tableName": "`devenv`.`sdp`.order_customer_delta_sink"}
)


@dp.append_flow(target="delta_sink")
def append_to_delta_sink():
    return spark.readStream.table("orders_customer_joined")