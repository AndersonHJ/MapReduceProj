REGISTER file:/usr/local/pig/lib/piggybank.jar
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

flights = load '$INPUT' using CSVLoader as (Year:int,
Quarter:int,
Month:int,
DayofMonth:int,
DayOfWeek:int,
FlightDate:Datetime,
UniqueCarrier:chararray,
AirlineID:long,
Carrier:chararray,
TailNum:chararray,
FlightNum:chararray,
Origin:chararray,
OriginCityName:chararray,
OriginState:chararray,
OriginStateFips:chararray,
OriginStateName:chararray,
OriginWac:int,
Dest:chararray,
DestCityName:chararray,
DestState:chararray,
DestStateFips:chararray,
DestStateName:chararray,
DestWac:int,
CRSDepTime:chararray,
DepTime:long,
DepDelay:double,
DepDelayMinutes:double,
DepDel15:double,
DepartureDelayGroups:int,
DepTimeBlk:chararray,
TaxiOut:int,
WheelsOff:chararray,
WheelsOn:chararray,
TaxiIn:int,
CRSArrTime:chararray,
ArrTime:long,
ArrDelay:double,
ArrDelayMinutes:double,
ArrDel15:double,
ArrivalDelayGroups:int,
ArrTimeBlk:chararray,
Cancelled:int,
CancellationCode:chararray,
Diverted:int,
CRSElapsedTime:double,
ActualElapsedTime:double,
AirTime:double,
Flights:double,
Distance:double,
DistanceGroup:long,
CarrierDelay:double,
WeatherDelay:double,
NASDelay:long,
SecurityDelay:long,
LateAircraftDelay:long);


flightsValid = FOREACH flights GENERATE 
        FlightNum, Year, Month, Origin, Dest, FlightDate, ArrTime, DepTime, Cancelled, Diverted, ArrDelayMinutes;

filtered_valid_flights = FILTER flightsValid BY 
        (Year < 1988 OR (1988 == Year AND 12 >= Month))
        AND (Year > 1988 OR (Year == 1988 AND 4 <= Month))
        AND Cancelled != 1
        AND Diverted != 1
        AND Origin == 'ORD';

filtered_valid_flights2 = FILTER flightsValid BY 
        (Year < 1988 OR (1988 == Year AND 12 >= Month))
        AND (Year > 1988 OR (Year == 1988 AND 4 <= Month))
        AND Cancelled != 1
        AND Diverted != 1
        AND Dest == 'JFK';

results = JOIN filtered_valid_flights BY (FlightDate, Dest), filtered_valid_flights2 BY (FlightDate, Origin);

filtered_results = FILTER results BY filtered_valid_flights::ArrTime < filtered_valid_flights2::DepTime;

dump filtered_results;
STORE filtered_results INTO '$OUTPUT' USING PigStorage (',');

sum_results = FOREACH filtered_results GENERATE *, filtered_valid_flights::ArrDelayMinutes 
            + filtered_valid_flights2::ArrDelayMinutes as totalDelay;

groupedData = GROUP sum_results ALL;

sum = FOREACH groupedData GENERATE AVG(sum_results.totalDelay);

dump sum;