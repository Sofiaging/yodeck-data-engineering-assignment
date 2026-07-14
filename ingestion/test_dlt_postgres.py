import dlt


pipeline = dlt.pipeline(
    pipeline_name="yodeck_test",
    destination="postgres",
    dataset_name="raw"
)


data = [
    {
        "account_id": 1,
        "name": "Test Company",
        "country": "Greece"
    },
    {
        "account_id": 2,
        "name": "Demo Company",
        "country": "Germany"
    }
]


load_info = pipeline.run(
    data,
    table_name="accounts"
)


print(load_info)
