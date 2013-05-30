package prerna.search;

import java.util.StringTokenizer;

import org.apache.jena.larq.IndexBuilderString;
import org.apache.lucene.index.IndexWriter;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.rdf.model.Statement;

public class SubjectIndexer extends IndexBuilderString{

	
	IndexWriter writer = null;
	
	 public SubjectIndexer(IndexWriter writer)
	 {
		 super(writer);
		 //index = writer;
	 }
	 
	 
	 
	@Override
	public void indexStatement(Statement statement){
		// TODO Auto-generated method stub
		// get the subject
		// strip it all the way and 
		// index it
		super.indexStatement(statement);
		String subject = statement.getSubject() + "";
		indexString(statement.getSubject().asNode(), subject);
		String predicate = statement.getPredicate() + "";
		indexString(statement.getPredicate().asNode(), predicate);
		String object = statement.getObject() + "";
		indexString(statement.getObject().asNode(), object);
	}
	
	public void indexString(Node node, String data)
	{
		StringTokenizer tokens = new StringTokenizer(data, "/");
		
		// get rid of the first token
		if(data.startsWith("http:"))
			tokens.nextElement();

		while(tokens.hasMoreElements())
		{
			String indexStr = tokens.nextToken();
			//Node = new Node();
			index.index(node, indexStr);
			if(indexStr.contains("_")) // tokenize the damn thing and tokenize that too
			{
				StringTokenizer tokens2 = new StringTokenizer(indexStr, "_");
				while(tokens2.hasMoreElements())
					index.index(node, tokens2.nextToken());
			
			}	
		}            
	}

	@Override
	public void unindexStatement(Statement statement) {
		// TODO Auto-generated method stub
		
	}


}
