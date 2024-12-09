MAIN_CLASS=CHIJoin
INPUT_FILE1=project/crashes_chicago.csv
INPUT_FILE2=project/weather_chicago.csv
OUTPUT_DIR=project/output/chicago

HIVE_TABLE_DIR=project/crash_with_weather
HIVE_TABLE_FILE=crash_with_weather_chicago

hadoop fs -rm -r -f $OUTPUT_DIR
rm -f *.class *.jar

javac -classpath `hadoop classpath` *.java
jar cvf data.jar *.class
hadoop jar data.jar $MAIN_CLASS $INPUT_FILE $OUTPUT_DIR

hadoop fs -rm -f $HIVE_TABLE_DIR/$HIVE_TABLE_FILE
hadoop fs -mv $OUTPUT_DIR/part-r-00000 $HIVE_TABLE_DIR/$HIVE_TABLE_FILE