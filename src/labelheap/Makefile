#JDKPATH = /s/java
#LIBPATH = /p/course/cs784-raghu/minibase_share/javaminibase/construction
JDKPATH = /usr
LIBPATH = .:..
CLASSPATH = $(JDKPATH)/lib/classes.zip:.:..:$(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

PROGS = together

all: $(PROGS)

lhf:LabelHeapfile.java
	$(JAVAC) LabelHeapfile.java

lhp:LHFPage.java
	$(JAVAC) HFPage.java

triple:Triple.java
	$(JAVAC) Triple.java

lscan:LScan.java
	$(JAVAC) LScan.java

together:*.java
	$(JAVAC) *.java

clean:
	\rm -f *.class *~ \#* core 
