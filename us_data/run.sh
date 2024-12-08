MAIN_CLASS=US
INPUT_FILE=project/crashes_us.csv
OUTPUT_DIR=project/output/us

javac -classpath `hadoop classpath` *.java
jar cvf us.jar *.class
hadoop jar us.jar $MAIN_CLASS $INPUT_FILE $OUTPUT_DIR
hadoop fs -tail $OUTPUT_DIR/part-r-00000