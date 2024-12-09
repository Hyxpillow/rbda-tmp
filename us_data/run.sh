MAIN_CLASS=US
INPUT_FILE=project/crashes_us.csv
OUTPUT_DIR=project/output/us

HIVE_TABLE_DIR=project/crash_with_weather
HIVE_TABLE_FILE=crash_with_weather_us

hadoop fs -rm -r -f $OUTPUT_DIR
rm -f *.class *.jar

javac -classpath `hadoop classpath` *.java
jar cvf data.jar *.class
hadoop jar data.jar $MAIN_CLASS $INPUT_FILE $OUTPUT_DIR

hadoop fs -rm -f $HIVE_TABLE_DIR/$HIVE_TABLE_FILE
hadoop fs -mv $OUTPUT_DIR/part-r-00000 $HIVE_TABLE_DIR/$HIVE_TABLE_FILE