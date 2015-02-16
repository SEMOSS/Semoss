/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.rdf.main;

import java.io.InputStream;

import prerna.util.Constants;
import prerna.util.DIHelper;

import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.util.FileManager;

/**
 */
public class RDFFileReader {

	/**
	 * Method main.
	 * @param args String[]
	 */
	public static void main(String [] args)
	{
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String fileName = workingDir + "/Sample3.rdf";

		System.out.println("Completed loading file ");
		InputStream in = FileManager.get().open(fileName); 			
		System.out.println("Completed loading file ");
		Model jenaModel = ModelFactory.createDefaultModel();//  .read(in, null); //, rdfFileType);
		jenaModel.read(in, null, "RDF/XML");
		
		//com.hp.hpl.jena.query.Query q2 = QueryFactory.create(" ?s ?p ?o WHERE {?s ?p ?o}"); 
		//com.hp.hpl.jena.query.ResultSet rs = QueryExecutionFactory.create(q2, jenaModel).execSelect();
		
		StmtIterator sti = jenaModel.listStatements();
		System.out.println("Iterating " + jenaModel.isEmpty());
	
		String query = "SELECT ?subject ?predicate ?object WHERE {" +
				//"{?subject <" +  RDFS. + "> <http://sandbox-api.smartplatforms.org/records/2169591> ;}" +
				//"BIND(<http://sandbox-api.smartplatforms.org/records/2169591>  AS ?subject )." +
				"?subject ?predicate ?object." +
				"BIND <http://sandbox-api.smartplatforms.org/records/2169591> AS ?subject." +
				"}";
		
		//com.hp.hpl.jena.query.Query queryVar = QueryFactory.create(query) ;
		com.hp.hpl.jena.query.ResultSet rs = QueryExecutionFactory.create(query, jenaModel).execSelect();

		/*QueryExecution qexec = QueryExecutionFactory.create(queryVar, jenaModel) ;
		Model resultModel = qexec.execConstruct() ;
		sti = resultModel.listStatements();

		
		while(sti.hasNext())
		{
			Statement stmt = sti.next();
			System.out.println("Statement " + stmt.getSubject() + "<>" + stmt.getPredicate() + "<>" + stmt.getObject());
		}*/
		while(rs.hasNext())
		{
			QuerySolution soln = rs.next();
			System.out.println(soln.get("?subject"));
		}
		
	}
	
}
