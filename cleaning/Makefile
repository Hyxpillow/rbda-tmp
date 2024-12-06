
MAIN_CLASS = CHICrashJoinWeather
INPUT_FILE1 = project/crashes.csv
INPUT_FILE2 = project/weather.csv
OUTPUT_DIR = project/output/join

.PHONY: all clean

all:
	javac -classpath `hadoop classpath` *.java
	jar cvf join.jar *.class

run: clean all
	hadoop jar join.jar $(MAIN_CLASS) $(INPUT_FILE1) $(INPUT_FILE2) $(OUTPUT_DIR)
	hadoop fs -cat $(OUTPUT_DIR)/part-r-00000

clean:
	hadoop fs -rm -r project/output
	rm -f *.class weather.jar
