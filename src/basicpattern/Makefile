JDKPATH = /usr
LIBPATH = /home/mugdhako/CSE510/minjava/javaminibase/construction
CLASSPATH = $(JDKPATH)/lib/classes.zip:.:..:$(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

PROGS = bp

all: $(PROGS)

bp:BasicPattern.java
	$(JAVAC) BasicPattern.java

clean:
	\rm -f *.class *~ \#* core 
