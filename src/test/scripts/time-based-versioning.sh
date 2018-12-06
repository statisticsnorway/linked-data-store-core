# Create resource R1 at december 6th at 8:14 in the morning
curl -X PUT "http://localhost:9090/ns/provisionagreement/R1?sync=true&timestamp=2018-12-06T08%3A14%3A53.000%2B01" -H "content-type: application/json; charset=utf-8" -d '{"name":"version1"}'

# Correct content of resource R1 at december 6th at 13:31 after lunch the same day, this will generate a new version of the same resource
curl -X PUT "http://localhost:9090/ns/provisionagreement/R1?sync=true&timestamp=2018-12-06T13%3A31%3A02.000%2B01" -H "content-type: application/json; charset=utf-8" -d '{"name":"version2"}'

# Get resource R1 as it were at december 6th at 07:00 in the morning, observe that no R1 resource exist at that time and 404 is returned
curl "http://localhost:9090/ns/provisionagreement/R1?sync=true&timestamp=2018-12-06T14%3A00%3A00.000%2B01" --write-out "%{http_code}\n" -o /dev/null -s

# Get resource R1 as it were at december 6th at 9:00 in the morning, observe that version1 is returned
curl "http://localhost:9090/ns/provisionagreement/R1?sync=true&timestamp=2018-12-06T09%3A00%3A00.000%2B01"

# Get resource R1 as it were at december 6th at 14:00 in the afternoon, observe that version2 is returned
curl "http://localhost:9090/ns/provisionagreement/R1?sync=true&timestamp=2018-12-06T14%3A00%3A00.000%2B01"
