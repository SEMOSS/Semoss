/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.search;

import java.util.StringTokenizer;

import org.apache.jena.larq.IndexBuilderString;
import org.apache.lucene.index.IndexWriter;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.rdf.model.Statement;

/**
 * Produce the indexing to search through a graph
 */
public class SubjectIndexer extends IndexBuilderString{


	IndexWriter writer = null;

	/**
	 * Constructor for SubjectIndexer
	 * @param writer 	IndexWriter to index the statement
	 */
	public SubjectIndexer(IndexWriter writer)
	{
		super(writer);
	}

	/**
	 * Indexes a statement by indexing the subject, predicate, and object 
	 * @param statement 	Statement containing the triple to index
	 */
	@Override
	public void indexStatement(Statement statement){
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

	/**
	 * Indexes a based based on its URI
	 * @param node 	The subject, predicate, or object of the statement as a node 
	 * @param data 	String containing the URI for the node
	 */
	public void indexString(Node node, String data)
	{
		StringTokenizer tokens = new StringTokenizer(data, "/");
		// get rid of the first token
		if(data.startsWith("http:")){
			tokens.nextElement();
		}
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

	/**
	 * Override method from IndexBuilderString
	 * @param statement Statement
	 */
	@Override
	public void unindexStatement(Statement statement) {

	}
}
