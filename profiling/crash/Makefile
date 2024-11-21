
.PHONY: all clean

all:
	javac -classpath hadoop classpath *.java
	jar cvf crash.jar *.class

clean:
	rm -f *.class crash.jar
