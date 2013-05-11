package LuceneHDFS;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.document.Document;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

public class Search extends EvalFunc<DataBag>
{
	IndexSearcher searcher;
	public DataBag exec(Tuple input) throws IOException
	{
		if (input == null || input.size() == 0)
			return null;

		long startTime = System.currentTimeMillis();
		// initialize searcher object for each node
		if (searcher == null)
		{
			Configuration config = new Configuration();
			config.set("fs.default.name","hdfs://localhost:9000/");
			FileSystem dfs = FileSystem.get(config);

			RAMDirectory rdir = new RAMDirectory();
			Path pth = new Path(dfs.getWorkingDirectory() + "/HDFS_DIR_NAME/");
			FileStatus[] filelst = dfs.listStatus(pth);
			FSDataInputStream filereader = null;
			for (int i = 0; i < filelst.length; i++)
			{
				log.warn("File Name:"	+ filelst[i].getPath().getName());
				filereader = dfs.open(filelst[i].getPath());
				int size = filereader.available();

				byte[] bytarr = new byte[size];
				filereader.read(bytarr, 0, size);

				IndexOutput indxout = rdir.createOutput(filelst[i].getPath().getName());
				indxout.writeBytes(bytarr, bytarr.length);
				indxout.flush();
				indxout.close();
			}
			filereader.close();

			//initialize from HDFS 
			searcher = new IndexSearcher(rdir);

		} // initialized

		
		DataBag bagOfTokens = BagFactory.getInstance().newDefaultBag();
		try
		{
			String tmpQuery = (String) input.get(0);
			String strQuery = "field2:\"" + tmpQuery + "\"";
			QueryParser queryParser = new QueryParser(Version.LUCENE_34, "field2", new StandardAnalyzer(Version.LUCENE_34));
			Query query = queryParser.parse(strQuery);
			TopScoreDocCollector collector = TopScoreDocCollector.create(500,true);

			searcher.search(query, collector);
			log.warn("Query >> " + query.toString() + " Time Taken:" + (System.currentTimeMillis() - startTime));
			ScoreDoc[] hits = collector.topDocs().scoreDocs;
			String str = Integer.toString(hits.length);

			for (int i = 0; i < hits.length; ++i)
			{
				int docId = hits[i].doc;
				Document d = searcher.doc(docId);
				str +=  "|" + hits[i].score + "|" + d.get("field1") + "|" + d.get("field2");
				
				Tuple termText = TupleFactory.getInstance().newTuple(str);
				bagOfTokens.add(termText);
			}
			
			return bagOfTokens;
		} 
		catch (Exception e)
		{
			//not handled yet!
		}
		return null;
	}

	public void finish()
	{
		try
		{
			searcher.close();
		} 
		catch (IOException i)
		{
			//not handled yet!
		}
	}
}
