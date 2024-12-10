set hive.execution.engine=mr;
set hive.fetch.task.conversion=minimal;

use yh5047_nyu_edu;

/* Create table according to the cleaned datasets */
create external table crash_with_weather (crash_date string, temperature double, humidity double, visibility double, rain_or_not tinyint)
row format delimited fields terminated by ','
location '/user/yh5047_nyu_edu/project/crash_with_weather/';

/* Impact of Rainfall */
SELECT COUNT(*), rain_or_not FROM crash_with_weather GROUP BY rain_or_not;

/* Impact of Temperature */
SELECT 
    CASE 
        WHEN temperature < 0 THEN 'below 0'
        WHEN temperature BETWEEN 0 AND 5 THEN '0-5'
        WHEN temperature BETWEEN 6 AND 10 THEN '6-10'
        WHEN temperature BETWEEN 11 AND 15 THEN '11-15'
        WHEN temperature BETWEEN 16 AND 20 THEN '16-20'
        WHEN temperature BETWEEN 21 AND 25 THEN '21-25'
        WHEN temperature BETWEEN 26 AND 30 THEN '26-30'
        WHEN temperature BETWEEN 31 AND 35 THEN '31-35'
        ELSE 'above 35'
    END AS temp_range,
    COUNT(*) AS accident_count
FROM crash_with_weather
GROUP BY 
    CASE 
        WHEN temperature < 0 THEN 'below 0'
        WHEN temperature BETWEEN 0 AND 5 THEN '0-5'
        WHEN temperature BETWEEN 6 AND 10 THEN '6-10'
        WHEN temperature BETWEEN 11 AND 15 THEN '11-15'
        WHEN temperature BETWEEN 16 AND 20 THEN '16-20'
        WHEN temperature BETWEEN 21 AND 25 THEN '21-25'
        WHEN temperature BETWEEN 26 AND 30 THEN '26-30'
        WHEN temperature BETWEEN 31 AND 35 THEN '31-35'
        ELSE 'above 35'
    END
ORDER BY accident_count DESC;

/* Impact of humidity */
SELECT 
    CASE 
        WHEN humidity BETWEEN 0 AND 40 THEN '0-40'
        WHEN humidity BETWEEN 41 AND 60 THEN '41-60'
        WHEN humidity BETWEEN 61 AND 80 THEN '61-80'
        ELSE '81-100'
    END AS humidity_range,
    COUNT(*) AS accident_count
FROM crash_with_weather
GROUP BY 
    CASE 
        WHEN humidity BETWEEN 0 AND 40 THEN '0-40'
        WHEN humidity BETWEEN 41 AND 60 THEN '41-60'
        WHEN humidity BETWEEN 61 AND 80 THEN '61-80'
        ELSE '81-100'
    END
ORDER BY accident_count DESC;
