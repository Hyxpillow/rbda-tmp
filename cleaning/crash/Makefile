
MAIN_CLASS = CHICrash
INPUT_FILE = project/crashes.csv
OUTPUT_DIR = project/output/crash

.PHONY: all clean

all:
	javac -classpath `hadoop classpath` *.java
	jar cvf crash.jar *.class

run: all
	hadoop jar crash.jar $(MAIN_CLASS) $(INPUT_FILE) $(OUTPUT_DIR)
	hadoop fs -cat project/output/crash/part-r-00000

clean:
	hadoop fs -rm -r project/output
	rm -f *.class crash.jar
