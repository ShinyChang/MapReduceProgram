JarFile="MRP.jar"
MainFunc="com.mrp.main.Main"
LocalOutDir="MRP/output"

all:help
jar:
	jar -cvf ${JarFile} -C bin/ .

run:
	jar -cvf ${JarFile} -C bin/ .
	hadoop jar ${JarFile} ${MainFunc} $(q) 

clean:
	hadoop fs -rmr MRP/output
	hadoop fs -rmr MRP/bf_output
	hadoop fs -rmr MRP/SnG_output
	hadoop fs -rmr MRP/merge_output

output:
	rm -rf ${LocalOutDir}
	hadoop fs -get output ${LocalOutDir}
	gedit ${LocalOutDir}/part-r-00000 & 

help:
	@echo "Usage:"
	@echo " make jar     - Build Jar File."
	@echo " make clean   - Clean up Output directory on HDFS."
	@echo " make run     - Run your MapReduce code on Hadoop."
	@echo " make output  - Download and show output file"
	@echo " make help    - Show Makefile options."
	@echo " "
	@echo "Example:"
	@echo " make jar; make run; make output; make clean"
