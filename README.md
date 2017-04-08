#Keboola Extractor for Rubicon Project Performance Analytics API

Parameters:
```javascript
{
    "basicAuth": "string", //required - The HTTP Basic authentication scheme ...
    "account": "string", //required - The Rubicon account ID, passed as “publisher/<accountId>”, for sample publisher/12345.
    "start": "2015-11-07T00:00:00-08:00", // optional
    "end": "2015-11-07T23:59:59-08:00", // optional
    // Start/end date in ISO-8601 format, including time zone.
    // if start/end date is not set start/end dates are created for 1 day back in timezone Europa/Prague
   
    "columnsToReturn": [
        "Site ID",
        "Avg. Bid CPM"
    ], 
    // required - min 1 dimension + min 1 metric
    // for "onlyDimensionOutput": true, / required 
    // value are Labels or API column keys
    "onlyDimensionOutput": true, // optional - default false - remove metrics from output
    "addDateToOutput": true, // optional - default false - add date column to the output file 
    "dimensionsFilter": [
        "Zone ID"
    ], 
    // optional 
    // 1. ask for dimension(s)
    // 2. loop for each dimension value and send requests with this value as filter
    "metricFilter": [
        "Auctions"
    ], // optional
    "currency": "USD", // optional
    "limit": 100000 // optional
}

```    
