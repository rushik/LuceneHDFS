LuceneHDFS
==========

Export Lucene indexes to HDFS and search Lucene indexes thru custom PIG UDF 

JAR Dependencies

- lucene-core-3.4.0.jar
- pig-0.10.0.jar
- hadoop-core-1.0.4.jar

Example Files

- IndexBuilderRAMDir.java : Indexes local documents and export lucene indexes to HDFS 
- Search.java : PIG UDF loads lucene indexes (one time) and search thru indexes and return BAG of matched documents 
- SearchHDFSRamDir.java : Example search code which loads indexes stored in HDFS and print matched documents 

*need to replace HDFS path, local path in above files