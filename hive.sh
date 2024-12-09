hadoop fs -mkdir /user/yh5047_nyu_edu/project/crash_with_weather/
hadoop fs -mv /user/yh5047_nyu_edu/project/output/chicago/part-r-00000 /user/yh5047_nyu_edu/project/crash_with_weather/crash_with_weather_chicago
hadoop fs -mv /user/yh5047_nyu_edu/project/output/us/part-r-00000 /user/yh5047_nyu_edu/project/crash_with_weather/crash_with_weather_us

beeline -u jdbc:hive2://localhost:10000
set hive.execution.engine=mr;
set hive.fetch.task.conversion=minimal;
use yh5047_nyu_edu;

create external table crash_with_weather (crash_date string, temperature double, humidity double, visibility double, rain_or_not tinyint)
row format delimited fields terminated by ','
location '/user/yh5047_nyu_edu/project/output/part-r-00000';

SELECT COUNT(*) FROM crash_with_weather;

SELECT COUNT(*) FROM crash_with_weather GROUP BY rain_or_not;