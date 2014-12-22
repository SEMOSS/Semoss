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
package prerna.rdf.query.util;

import java.util.ArrayList;

import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

public class SEMOSSQueryTester {
	public static void main(String [] args)
	{
		int test = 4;
		if (test ==1)
		{
			SEMOSSQuery sq = new SEMOSSQuery();
			sq.setQueryType(SPARQLConstants.SELECT);
			TriplePart systemVar = new TriplePart("System", TriplePart.VARIABLE);
			TriplePart dataVar = new TriplePart("Data", TriplePart.VARIABLE);
			TriplePart predVar = new TriplePart("Provide", TriplePart.VARIABLE);
			TriplePart predVar2 = new TriplePart("Provide2", TriplePart.VARIABLE);
			TriplePart systemTypeURI = new TriplePart("http://semoss.org/ontologies/Concept/System", TriplePart.URI);
			TriplePart dataTypeURI = new TriplePart("http://semoss.org/ontologies/Concept/DataObject", TriplePart.URI);
			TriplePart predicateURI = new TriplePart("http://semoss.org/ontologies/Relation/Provide", TriplePart.URI);
			TriplePart typeURI = new TriplePart(SPARQLConstants.TYPE_URI, TriplePart.URI);
			TriplePart subPropURI = new TriplePart(SPARQLConstants.SUBPROP_URI, TriplePart.URI);
			TriplePart bluTypeURI = new TriplePart("http://semoss.org/ontologies/Concept/BusinessLogicUnit", TriplePart.URI);
	//		Date objectVal = new Date();
	//		objectVal.setYear(2000);
	//		objectVal.setMonth(2);
	//		objectVal.setDate(10);
			
			//first add return vars
			sq.addSingleReturnVariable(systemVar);
			sq.addSingleReturnVariable(dataVar);
	
			
			//try just sys, data
			sq.addTriple(systemVar, typeURI, systemTypeURI);
			sq.addTriple(dataVar, typeURI, dataTypeURI);
			sq.addTriple(predVar, subPropURI, predicateURI);
			sq.addTriple(systemVar, predVar, dataVar);
			sq.createQuery();
			System.out.println(sq.getQuery());
			
			//now try sys, data, and CRM
			TriplePart CRMPropURI = new TriplePart("http://semoss.org/ontologies/Relation/Contains/CRM", TriplePart.URI);
			TriplePart crmVar = new TriplePart("crm", TriplePart.VARIABLE);
			sq.addSingleReturnVariable(crmVar);
			sq.addTriple(predVar, CRMPropURI, crmVar);
			sq.createQuery();
			System.out.println(sq.getQuery());
			
			//now try union
			sq = new SEMOSSQuery();
			TriplePart bluDataTypeVar = new TriplePart("bluData", TriplePart.VARIABLE);
			sq.setQueryType(SPARQLConstants.SELECT);
			sq.setDisctinct(true);
			sq.addSingleReturnVariable(systemVar);
			sq.addSingleReturnVariable(dataVar);
			sq.addSingleReturnVariable(bluDataTypeVar);
			sq.addTriple(systemVar, typeURI, systemTypeURI, "all");
			sq.addTriple(predVar, subPropURI, predicateURI, "dataUnion");
			sq.addTriple(dataVar, typeURI, dataTypeURI, "dataUnion");
			sq.addTriple(systemVar, predVar, dataVar, "dataUnion");
			sq.addBind(dataTypeURI, bluDataTypeVar, "dataUnion");
			sq.addTriple(predVar2, subPropURI, predicateURI, "bluUnion");
			sq.addTriple(dataVar, typeURI, bluTypeURI, "bluUnion"); //it's datavar here too because no coalesce in return, will use dataVar for both in separate Unions
			sq.addTriple(systemVar, predVar2, dataVar, "bluUnion");
			sq.addBind(bluTypeURI, bluDataTypeVar, "bluUnion");
			sq.setCustomQueryStructure("all {dataUnion} UNION {bluUnion}");
			sq.createQuery();
			System.out.println(sq.getQuery());
		}
		else if (test ==2)
		{
			SEMOSSQuery sq = new SEMOSSQuery();
			sq.setQueryType(SPARQLConstants.SELECT);
			String system = "System";
			String data = "DataObject";
			String provide = "Provide";
			String systemURI = "http://semoss.org/ontologies/Concept/System";
			String dataURI = "http://semoss.org/ontologies/Concept/DataObject";
			String provideURI = "http://semoss.org/ontologies/Relation/Provide";
			
			//first add return vars
			SEMOSSQueryHelper.addSingleReturnVarToQuery(system, sq);
			SEMOSSQueryHelper.addSingleReturnVarToQuery(data, sq);
			
			//add in triples to query
			SEMOSSQueryHelper.addConceptTypeTripleToQuery(system, systemURI, sq);
			SEMOSSQueryHelper.addConceptTypeTripleToQuery(data, dataURI, sq);
			SEMOSSQueryHelper.addRelationTypeTripleToQuery(provide, provideURI, sq);
			SEMOSSQueryHelper.addRelationshipVarTripleToQuery(system, provide, data, sq);
			
			sq.createQuery();
			System.out.println(sq.getQuery());
			
			//now try sys, data, and CRM
			String crm = "crm";
			String crmURI = "http://semoss.org/ontologies/Relation/Contains/CRM";
			SEMOSSQueryHelper.addSingleReturnVarToQuery(crm, sq);
			SEMOSSQueryHelper.addGenericTriple(provide, TriplePart.VARIABLE, crmURI, TriplePart.URI, crm, TriplePart.VARIABLE, sq);
			sq.createQuery();
			System.out.println(sq.getQuery());
			
			//now try union
			sq = new SEMOSSQuery();
			String bluDataType = "dataBLU";
			String bluURI = "http://semoss.org/ontologies/Concept/BusinessLogicUnit";
			sq.setQueryType(SPARQLConstants.SELECT);
			sq.setDisctinct(true);
			SEMOSSQueryHelper.addSingleReturnVarToQuery(system, sq);
			SEMOSSQueryHelper.addSingleReturnVarToQuery(data, sq);
			//SEMOSSQueryHelper.addSingleReturnVarToQuery(bluDataType, sq);
			SEMOSSQueryHelper.addConceptTypeTripleToQuery(system, systemURI, sq, "all");
			SEMOSSQueryHelper.addConceptTypeTripleToQuery(data, dataURI, sq, "dataUnion");
			SEMOSSQueryHelper.addRelationTypeTripleToQuery(provide, provideURI, sq, "dataUnion");
			SEMOSSQueryHelper.addRelationshipVarTripleToQuery(system, provide, data, sq, "dataUnion");
			//SEMOSSQueryHelper.addBindPhrase("Data", TriplePart.LITERAL, bluDataType, sq, "dataUnion");
			SEMOSSQueryHelper.addConceptTypeTripleToQuery(data, bluURI, sq, "bluUnion");
			SEMOSSQueryHelper.addRelationTypeTripleToQuery(provide+"2", provideURI, sq, "bluUnion");
			SEMOSSQueryHelper.addRelationshipVarTripleToQuery(system, provide+"2", data, sq, "bluUnion");
			SEMOSSQueryHelper.addBindPhrase("BLU", TriplePart.LITERAL, bluDataType, sq, "bluUnion");
			//
			sq.setCustomQueryStructure("all {dataUnion} UNION {bluUnion}");
			sq.createQuery();
			System.out.println(sq.getQuery());
		}
		else if(test ==3)
		{
			SEMOSSQuery sq = new SEMOSSQuery();
			sq.setQueryType(SPARQLConstants.SELECT);
			sq.setDisctinct(true);
			String cap = "Capability";
			String bp = "BusinessProcess";
			String act = "Activity";
			String data = "DataObject";
			String support = "Support";
			String consist = "Consist";
			String need = "Need";
			String capURI = "http://semoss.org/ontologies/Concept/Capability";
			String bpURI = "http://semoss.org/ontologies/Concept/BusinessProcess";
			String actURI = "http://semoss.org/ontologies/Concept/Activity";
			String dataURI = "http://semoss.org/ontologies/Concept/DataObject";
			String supportURI = "http://semoss.org/ontologies/Relation/Supports";
			String consistURI = "http://semoss.org/ontologies/Relation/Consists";
			String needURI = "http://semoss.org/ontologies/Relation/Needs";
			String weight1 = "weight1";
			String weight2 = "weight2";
			String weight3 = "weight3";
			String weightURI = "http://semoss.org/ontologies/Relation/Contains/weight";


			SEMOSSQueryHelper.addSingleReturnVarToQuery(cap, sq);
			SEMOSSQueryHelper.addSingleReturnVarToQuery(weight1, sq);	
			SEMOSSQueryHelper.addSingleReturnVarToQuery(bp, sq);
			SEMOSSQueryHelper.addSingleReturnVarToQuery(weight2, sq);	
			SEMOSSQueryHelper.addSingleReturnVarToQuery(act, sq);
			SEMOSSQueryHelper.addSingleReturnVarToQuery(weight3, sq);	
			SEMOSSQueryHelper.addSingleReturnVarToQuery(data, sq);
			
			SEMOSSQueryHelper.addConceptTypeTripleToQuery(cap, capURI, sq);
			SEMOSSQueryHelper.addConceptTypeTripleToQuery(bp, bpURI, sq);
			SEMOSSQueryHelper.addConceptTypeTripleToQuery(act, actURI, sq);
			SEMOSSQueryHelper.addConceptTypeTripleToQuery(data, dataURI, sq);
			SEMOSSQueryHelper.addRelationTypeTripleToQuery(support, supportURI, sq);
			SEMOSSQueryHelper.addRelationTypeTripleToQuery(consist, consistURI, sq);
			SEMOSSQueryHelper.addRelationTypeTripleToQuery(need, needURI, sq);
			SEMOSSQueryHelper.addRelationshipVarTripleToQuery(cap, support, bp, sq);
			SEMOSSQueryHelper.addRelationshipVarTripleToQuery(bp, consist, act, sq);
			SEMOSSQueryHelper.addRelationshipVarTripleToQuery(act, need, data, sq);
			SEMOSSQueryHelper.addGenericTriple(support, TriplePart.VARIABLE, weightURI, TriplePart.URI, weight1, TriplePart.VARIABLE, sq);
			SEMOSSQueryHelper.addGenericTriple(consist, TriplePart.VARIABLE, weightURI, TriplePart.URI, weight2, TriplePart.VARIABLE, sq);
			SEMOSSQueryHelper.addGenericTriple(need, TriplePart.VARIABLE, weightURI, TriplePart.URI, weight3, TriplePart.VARIABLE, sq);

			sq.createQuery();
			System.out.println(sq.getQuery());

		}
		else
		{
			SEMOSSQuery sq = new SEMOSSQuery();
			sq.setQueryType(SPARQLConstants.SELECT);
			sq.setDisctinct(true);
			String cap = "Capability";
			String bp = "BusinessProcess";
			String act = "Activity";
			String data = "DataObject";
			String support = "Support";
			String consist = "Consist";
			String need = "Need";
			String capURI = "http://semoss.org/ontologies/Concept/Capability";
			String bpURI = "http://semoss.org/ontologies/Concept/BusinessProcess";
			String actURI = "http://semoss.org/ontologies/Concept/Activity";
			String dataURI = "http://semoss.org/ontologies/Concept/DataObject";
			String supportURI = "http://semoss.org/ontologies/Relation/Supports";
			String consistURI = "http://semoss.org/ontologies/Relation/Consists";
			String needURI = "http://semoss.org/ontologies/Relation/Needs";
			String weight1 = "weight1";
			String weight2 = "weight2";
			String weight3 = "weight3";
			String weightURI = "http://semoss.org/ontologies/Relation/Contains/weight";


			SEMOSSQueryHelper.addSingleReturnVarToQuery(cap, sq);
			SEMOSSQueryHelper.addSingleReturnVarToQuery(data, sq);
			//now time to try the modifiers
			ISPARQLReturnModifier mod;
			ArrayList<Object> list1 = new ArrayList<Object>();
			ArrayList<String> list2 = new ArrayList<String>();
			list1.add(weight1);
			list1.add(weight2);
			list1.add(weight3);
			list2.add("*");
			list2.add("*");
			//first multiply the weights
			mod = SEMOSSQueryHelper.createReturnModifier(list1, list2);
			//then sum the weights
			mod = SEMOSSQueryHelper.createReturnModifier(mod, SPARQLAbstractReturnModifier.SUM);
			//now multiply by 100
			list1 = new ArrayList<Object>();
			list2 = new ArrayList<String>();
			list1.add(mod);
			list1.add(100);
			list2.add("*");
			mod = SEMOSSQueryHelper.createReturnModifier(list1, list2);
			SEMOSSQueryHelper.addSingleReturnVarToQuery("weightOutput", mod, sq);

			SEMOSSQueryHelper.addConceptTypeTripleToQuery(cap, capURI, sq);
			SEMOSSQueryHelper.addConceptTypeTripleToQuery(cap, capURI, sq);
			SEMOSSQueryHelper.addConceptTypeTripleToQuery(bp, bpURI, sq);
			SEMOSSQueryHelper.addConceptTypeTripleToQuery(act, actURI, sq);
			SEMOSSQueryHelper.addConceptTypeTripleToQuery(data, dataURI, sq);
			SEMOSSQueryHelper.addRelationTypeTripleToQuery(support, supportURI, sq);
			SEMOSSQueryHelper.addRelationTypeTripleToQuery(consist, consistURI, sq);
			SEMOSSQueryHelper.addRelationTypeTripleToQuery(need, needURI, sq);
			SEMOSSQueryHelper.addRelationshipVarTripleToQuery(cap, support, bp, sq);
			SEMOSSQueryHelper.addRelationshipVarTripleToQuery(bp, consist, act, sq);
			SEMOSSQueryHelper.addRelationshipVarTripleToQuery(act, need, data, sq);
			SEMOSSQueryHelper.addGenericTriple(support, TriplePart.VARIABLE, weightURI, TriplePart.URI, weight1, TriplePart.VARIABLE, sq);
			SEMOSSQueryHelper.addGenericTriple(consist, TriplePart.VARIABLE, weightURI, TriplePart.URI, weight2, TriplePart.VARIABLE, sq);
			SEMOSSQueryHelper.addGenericTriple(need, TriplePart.VARIABLE, weightURI, TriplePart.URI, weight3, TriplePart.VARIABLE, sq);

			//add groupby at the end
			ArrayList<String> groupList  = new ArrayList<String>();
			groupList.add(cap);
			groupList.add(data);
			SEMOSSQueryHelper.addGroupByToQuery(groupList, sq);
			sq.createQuery();
			
			System.out.println(sq.getQuery());

		}
	}
}
