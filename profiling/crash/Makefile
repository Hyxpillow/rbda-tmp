
MAIN_CLASS = CHICrash
INPUT_FILE = project/crashes.csv
OUTPUT_DIR = project/output

.PHONY: all clean

all:
	javac -classpath `hadoop classpath` *.java
	jar cvf crash.jar *.class

run: all
	hadoop jar crash.jar $(MAIN_CLASS) $(INPUT_FILE) $(OUTPUT_DIR)

clean:
	rm -f *.class crash.jar
