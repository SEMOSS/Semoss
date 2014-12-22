/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.poi.main;

import java.util.ArrayList;
import java.util.Hashtable;

import prerna.error.EngineException;
import prerna.error.FileReaderException;
import prerna.error.FileWriterException;
import prerna.error.NLPException;

public class NLPReader extends AbstractFileReader {
	
	public Hashtable<String,Object> temp = new Hashtable<String,Object>(); 
	public Hashtable<String,Object> temp2 = new Hashtable<String,Object>();
	public Hashtable<String,Object> temp3 = new Hashtable<String,Object>();


	public void importFileWithOutConnection(String engineName, String fileNames, String customBase, String customMap, String owlFile) throws FileReaderException, EngineException, FileWriterException, NLPException {	
		String[] files = prepareReader(fileNames, customBase, owlFile);
		openEngineWithoutConnection(engineName);
		ArrayList <TripleWrapper> Triples = new ArrayList<TripleWrapper>();

		if(!customMap.equals("")) 
		{
			openProp(customMap);
		}
		//if user selected a map, load just as before--using the prop file to discover Excel->URI translation
		ProcessNLP docReader = new ProcessNLP();
		Triples = docReader.masterRead(files);
		createNLPrelationships(Triples);
		createBaseRelations();
		closeDB();

	}

	public void createNLPrelationships( ArrayList<TripleWrapper> Triples) throws EngineException {
		for(int i = 0; i < Triples.size(); i++){
			temp.put("occurance", Triples.get(i).getObj1num());
			temp2.put("occurance", Triples.get(i).getPrednum());
			temp3.put("occurance", Triples.get(i).getObj2num());
			createRelationship("subject", "predicate", Triples.get(i).getObj1(), Triples.get(i).getPred(),"subjectofpredicate", temp);
			createRelationship("predicate","object", Triples.get(i).getPred(), Triples.get(i).getObj2(),"predicateofobject", temp);
			createRelationship("object", "subject", Triples.get(i).getObj2(), Triples.get(i).getObj1(),"objectofsubject", temp2);

			addNodeProperties("subject",Triples.get(i).getObj1(),temp);
			addNodeProperties("predicate",Triples.get(i).getPred(),temp2);
			addNodeProperties("object",Triples.get(i).getObj2(),temp3);

			temp.remove("occurance");
			temp2.remove("occurance");
			temp3.remove("occurance");

			createRelationship("subject", "subjectexpanded", Triples.get(i).getObj1(), Triples.get(i).getObj1exp(),"expandedofsubject", temp);
			createRelationship("predicate", "predicateexpanded", Triples.get(i).getPred(), Triples.get(i).getPredexp(),"expandedofpredicate", temp);
			createRelationship("object", "objectexpanded", Triples.get(i).getObj2(), Triples.get(i).getObj2exp(),"expandedofobject", temp);

			createRelationship("subject", "articlenum", Triples.get(i).getObj1(), Triples.get(i).getArticleNum(),"articleofsubject", temp);
			createRelationship("predicate", "articlenum", Triples.get(i).getPred(), Triples.get(i).getArticleNum(),"articleofpredicate", temp);
			createRelationship("object", "articlenum", Triples.get(i).getObj2(), Triples.get(i).getArticleNum(),"articleofobject", temp);

			createRelationship("subject", "sentence", Triples.get(i).getObj1(), Triples.get(i).getSentence(),"sentenceofsubject", temp);
			createRelationship("predicate", "sentence", Triples.get(i).getPred(), Triples.get(i).getSentence(),"sentenceofpredicate", temp);
			createRelationship("object", "sentence", Triples.get(i).getObj2(), Triples.get(i).getSentence(),"sentenceofobject", temp);
		}
	}
}

