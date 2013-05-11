package LuceneHDFS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import java.io.*;

public class IndexBuilderRAMDir
{
	public static void main(String[] args) throws IOException
	{
		RAMDirectory rdir = new RAMDirectory();
		IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_34,	new StandardAnalyzer(Version.LUCENE_34));
		IndexWriter writer = new IndexWriter(rdir, config);
		
		BufferedReader b_reader = new BufferedReader(new FileReader(
				new File("<FILE_PATH - tab seperated records, each record considered as one document>")));
		String line;

		while ((line = b_reader.readLine()) != null)
		{
			Document doc = new Document();
			String datavalue[] = line.split("\t");

			try
			{
				System.out.println(datavalue[0] + " " + datavalue[1]);
			} 
			catch (Exception e)
			{
				System.out.println("Skipped...");
			}

			// with Term Vector
			doc.add(new Field("field1", datavalue[0], Field.Store.YES,
					Field.Index.ANALYZED, Field.TermVector.YES));

			doc.add(new Field("field2", datavalue[1], Field.Store.YES,
					Field.Index.ANALYZED, Field.TermVector.YES));
			
			writer.addDocument(doc);
		}

		int newNumDocs = writer.numDocs();
		System.out.println(newNumDocs + " documents added.");
		
		writer.optimize();
		writer.close();
		b_reader.close();
		
		//Parse RamDirectory and write to HDFS
		
		String Index_DIR="/HDFS_DIR_NAME/";
		Configuration hdConfig = new Configuration();
		hdConfig.set("fs.default.name","hdfs://localhost:9000/");
		FileSystem dfs = FileSystem.get(hdConfig);
		
		String fileList[] = rdir.listAll();
		for (int i = 0; i < fileList.length; i++)
		{
		    IndexInput indxfile = rdir.openInput(fileList[i].trim());
		    long len = indxfile.length();
		    int len1 = (int) len;
		 
		    byte[] bytarr = new byte[len1];
		    indxfile.readBytes(bytarr, 0, len1);
		    
		    System.out.println("File Name:" + fileList[i].trim());
		    System.out.println("File Content:" + indxfile.toString());
		    
		    Path src = new Path(dfs.getWorkingDirectory()+Index_DIR+ fileList[i].trim());
		    dfs.createNewFile(src);
		 
		    // Writing data from byte array to the file in HDFS
		    FSDataOutputStream fs = dfs.create(new Path(dfs.getWorkingDirectory()+Index_DIR+fileList[i].trim()),true);
		    fs.write(bytarr);
		    fs.close();
		}
		dfs.close();
	}
}