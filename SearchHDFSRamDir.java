package LuceneHDFS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import java.io.*;

public class SearchHDFSRamDir
{
	public static void main(String[] args) throws IOException, ParseException
	{
		Configuration config = new Configuration();
		config.set("fs.default.name","hdfs://localhost:9000/");
		FileSystem dfs = FileSystem.get(config);
		 
		RAMDirectory rdir = new RAMDirectory();
		String Index_DIR="/HDFS_DIR_NAME/";
		             
		// Getting the list of index files present in the directory into an array.
		Path pth = new Path(dfs.getWorkingDirectory()+Index_DIR);
		FileStatus[] filelst = dfs.listStatus(pth);
		FSDataInputStream filereader = null;
		for (int i = 0; i<filelst.length; i++)
		{
			System.out.println("File Name:"+filelst[i].getPath().getName());
			
			filereader = dfs.open(filelst[i].getPath());
		    int size = filereader.available();
		 
		    byte[] bytarr = new byte[size];
		    filereader.read(bytarr, 0, size);
		     
		    IndexOutput indxout = rdir.createOutput(filelst[i].getPath().getName());
		    indxout.writeBytes(bytarr,bytarr.length);
		    indxout.flush();        
		    indxout.close();
		                    
		}
		filereader.close();
		dfs.close();
		
		// Now Search
	    IndexSearcher searcher = new IndexSearcher(rdir);
	    String str_final = "field2:\"TESTTERM\"";
	    
		QueryParser queryParser = new QueryParser(Version.LUCENE_34, "field2", new StandardAnalyzer(Version.LUCENE_34));
	    Query query = queryParser.parse(str_final);
	    System.out.println("Query Data:" + query.toString());
	    TopScoreDocCollector collector = TopScoreDocCollector.create(500, true);
	    
	    // Search Operation
	    searcher.search(query, collector);
	    // Display Results
	    ScoreDoc[] hits = collector.topDocs().scoreDocs;
	    System.out.println("Found " + hits.length + " hits.");
	    for(int i=0; i < hits.length; ++i) 
	    {
	      int docId = hits[i].doc;
	      Document d = searcher.doc(docId);
    	  System.out.println((i + 1) + ". " + hits[i].score + " "+ d.get("field1") + " ==== " + d.get("field2"));
	    }
	    // Close the searcher object
	    searcher.close();
	}
}
