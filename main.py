from influxdb_client import InfluxDBClient, Dialect
import csv

client = InfluxDBClient.from_config_file("config.ini")

query_api = client.query_api()


## using csv library
csv_result = query_api.query_csv('from(bucket:"cifxdata") |> range(start: 2022-08-22T00:00:00Z, stop: 2022-08-22T23:59:59Z)', dialect=Dialect(header=True, delimiter=',', annotations=[], date_time_format="RFC3339"))

""" val_count = 0
for row in csv_result:
    #print(row)
    for cell in row:
        val_count += 1
print(val_count) """


with open('output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile, delimiter=',', dialect='unix')
    writer.writerows(csv_result)
    
csvfile.close()
client.close()