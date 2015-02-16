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
package prerna.poi.main;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import prerna.error.EngineException;
import prerna.error.FileReaderException;
import prerna.error.FileWriterException;
import prerna.error.NLPException;

public class NLPReader extends AbstractFileReader {
	
	private List<TripleWrapper> triples = new ArrayList<TripleWrapper>();
	
	public void importFileWithOutConnection(String engineName, String fileNames, String customBase, String customMap, String owlFile) throws FileReaderException, EngineException, FileWriterException, NLPException {	
		String[] files = prepareReader(fileNames, customBase, owlFile);
		openEngineWithoutConnection(engineName);
		
		if(!customMap.equals("")) 
		{
			openProp(customMap);
		}
		//if user selected a map, load just as before--using the prop file to discover Excel->URI translation
		ProcessNLP processor = new ProcessNLP();
		triples = processor.generateTriples(files);
		createNLPrelationships();
		createBaseRelations();
		closeDB();
	}

	public void createNLPrelationships() throws EngineException {
		String docNameConceptType = "ArticleName";
		String sentenceConceptType = "Sentence";
		String subjectConceptType = "Subject";
		String predicateConceptType = "Predicate";
		String objectConceptType = "Object";
		String subjectExpandedConceptType = "SubjectExpanded";
		String predicateExpandedConceptType = "PredicateExpanded";
		String objectExpandedConceptType = "ObjectExpanded";
		
		String subjectToPredicateRelationType = "SubjectOfPredicate";
		String predicateToObjectRelationType = "PredicateOfObject";
		String objectToSubjectRelationType = "ObjectOfSubject";
		String expandedOfSubjectRelationType = "ExpandedOfSubject";
		String expandedOfPredicateRelationType = "ExpandedOfPredicate";
		String expandedOfObjectRelationType = "ExpandedOfObject";
		String articleOfSubjectRelationType = "ArticleOfSubject";
		String articleOfPredicateRelationType = "ArticleOfPredicate";
		String articleOfObjectRelationType = "ArticleOfObject";
		String sentenceOfSubjectRelationType = "SentenceOfSubject";
		String sentenceOfPredicateRelationType = "SentenceOfPredicate";
		String sentenceOfObjectRelationType = "SentenceOfObject";
		
		String occurancePropKey = "occurance";

		Hashtable<String, Object> emptyHash = new Hashtable<String, Object>();

		int i = 0;
		int numTriples = triples.size();
		for(; i < numTriples; i++){
			Hashtable<String, Object> countHash = new Hashtable<String, Object>();
			countHash.put(occurancePropKey, triples.get(i).getObj1Count());
			createRelationship(subjectConceptType, predicateConceptType, triples.get(i).getObj1(), triples.get(i).getPred(), subjectToPredicateRelationType, countHash);
			addNodeProperties(subjectConceptType, triples.get(i).getObj1(), countHash);

			countHash.put(occurancePropKey, triples.get(i).getPredCount());
			createRelationship(predicateConceptType, objectConceptType, triples.get(i).getPred(), triples.get(i).getObj2(), predicateToObjectRelationType, countHash);
			addNodeProperties(predicateConceptType, triples.get(i).getPred(), countHash);

			countHash.put(occurancePropKey, triples.get(i).getObj2Count());
			createRelationship(objectConceptType, subjectConceptType, triples.get(i).getObj2(), triples.get(i).getObj1(), objectToSubjectRelationType, countHash);
			addNodeProperties(objectConceptType, triples.get(i).getObj2(), countHash);

			createRelationship(subjectConceptType, subjectExpandedConceptType, triples.get(i).getObj1(), triples.get(i).getObj1Expanded(), expandedOfSubjectRelationType, emptyHash);
			createRelationship(predicateConceptType, predicateExpandedConceptType, triples.get(i).getPred(), triples.get(i).getPredExpanded(), expandedOfPredicateRelationType, emptyHash);
			createRelationship(objectConceptType, objectExpandedConceptType, triples.get(i).getObj2(), triples.get(i).getObj2Expanded(), expandedOfObjectRelationType, emptyHash);

			createRelationship(subjectConceptType, docNameConceptType, triples.get(i).getObj1(), triples.get(i).getDocName(), articleOfSubjectRelationType, emptyHash);
			createRelationship(predicateConceptType, docNameConceptType, triples.get(i).getPred(), triples.get(i).getDocName(), articleOfPredicateRelationType, emptyHash);
			createRelationship(objectConceptType, docNameConceptType, triples.get(i).getObj2(), triples.get(i).getDocName(), articleOfObjectRelationType, emptyHash);

			createRelationship(subjectConceptType, sentenceConceptType, triples.get(i).getObj1(), triples.get(i).getSentence(), sentenceOfSubjectRelationType, emptyHash);
			createRelationship(predicateConceptType, sentenceConceptType, triples.get(i).getPred(), triples.get(i).getSentence(), sentenceOfPredicateRelationType, emptyHash);
			createRelationship(objectConceptType, sentenceConceptType, triples.get(i).getObj2(), triples.get(i).getSentence(), sentenceOfObjectRelationType, emptyHash);
		}
	}
}

