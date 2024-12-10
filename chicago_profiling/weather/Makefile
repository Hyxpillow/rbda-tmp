
MAIN_CLASS = CHIWeather
INPUT_FILE = project/weather.csv
OUTPUT_DIR = project/output/weather

.PHONY: all clean

all:
	javac -classpath `hadoop classpath` *.java
	jar cvf weather.jar *.class

run: clean all
	hadoop jar weather.jar $(MAIN_CLASS) $(INPUT_FILE) $(OUTPUT_DIR)
	hadoop fs -cat project/output/weather/part-r-00000

clean:
	hadoop fs -rm -r project/output
	rm -f *.class weather.jar
