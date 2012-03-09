#This Makefile is used to create assignments.

#the following macros should be updated according to
#the assignment to be generated

JARFILES=bufmgr/*.class diskmgr/*.class global/*.class iterator/*.class\
         heap/*.class chainexception/*.class  btree/*.class index/*.class tests/*.class tripleheap/*.class labelheap/*.class tripleiterator/*.class basicpattern/*.class basicpatterniterator/*.class

JDKPATH = /usr
LIBPATH = .:..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

DOCFILES=bufmgr diskmgr global chainexception heap btree iterator index tripleheap labelheap tripleiterators basicpattern basicpatterniterator

##############  update the above for each assignment in making

ASSIGN=.
LIBDIR=$(ASSIGN)/lib
KEY=$(ASSIGN)/key
SRC=$(ASSIGN)/src

IMAGELINK=$(PACKAGEINDEX)/images
PACKAGEINDEX=$(ASSIGN)/javadoc

JAVADOC=javadoc -public -d $(PACKAGEINDEX)

### Generate jar and javadoc files.  Apply to most assignments.
db: 
	make -C global
	make -C chainexception
	make -C btree
	make -C bufmgr
	make -C diskmgr
	make -C heap
	make -C index
	make -C iterator
	make -C labelheap
	make -C tripleheap
	make -C basicpattern
	make -C basicpatterniterator
	make -C tripleiterator
doc:
	$(JAVADOC) $(DOCFILES)

test: 
	cd tests; make bmtest dbtest; whoami; make hftest bttest indextest jointest sorttest sortmerge lhftest tftest

clean:
	\rm -f $(CLASSPATH)/*.class *~ \#* core $(JARFILES) TRACE

