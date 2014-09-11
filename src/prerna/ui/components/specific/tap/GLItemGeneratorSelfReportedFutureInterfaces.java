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
package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import javax.swing.JComboBox;
import javax.swing.JList;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.poi.main.POIWriter;
import prerna.poi.main.RelationshipLoadingSheetWriter;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * This class is used to generate items in the GL and is used to update the cost database.
 */
public class GLItemGeneratorSelfReportedFutureInterfaces {

	private IEngine hrCore;
	private IEngine futureDB;
	private IEngine futureCostDB;
	private IEngine tapCost;

	private String genSpecificDProtQuery = "SELECT DISTINCT ?icd ?data ?sys (COALESCE(?dprot, (URI(\"http://health.mil/ontologies/Concept/DProt/HTTPS-SOAP\"))) AS ?Prot) WHERE { {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;}  {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}BIND(<http://semoss.org/ontologies/Relation/Consume> AS ?downstream) {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?icd ?downstream ?sys ;} {?payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload> } {?icd ?payload ?data ;} OPTIONAL{ ?payload <http://semoss.org/ontologies/Relation/Contains/Protocol> ?dprot} FILTER(STR(?sys)!=\"http://health.mil/ontologies/Concept/System/DHMSM\")}";
	private String genSpecificDFormQuery = "SELECT DISTINCT ?icd ?data ?sys (COALESCE(?dform, (URI(\"http://health.mil/ontologies/Concept/DForm/XML\"))) AS ?Form) WHERE { {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;}  {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}BIND(<http://semoss.org/ontologies/Relation/Consume> AS ?downstream) {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?icd ?downstream ?sys ;} {?payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload> } {?icd ?payload ?data ;} OPTIONAL{ ?payload <http://semoss.org/ontologies/Relation/Contains/Format> ?dform} FILTER(STR(?sys)!=\"http://health.mil/ontologies/Concept/System/DHMSM\")}";
	private String providerDataQuery1 = "SELECT DISTINCT ?icd ?data ?sys WHERE { {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;}  {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}BIND(<http://semoss.org/ontologies/Relation/Provide> AS ?upstream) {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?sys ?upstream ?icd ;} {?payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload> } {?icd ?payload ?data ;}  FILTER(STR(?sys)!=\"http://health.mil/ontologies/Concept/System/DHMSM\")}";
	private String providerDataQuery2 = "";
	private String consumerDataQuery = "SELECT DISTINCT ?icd ?data ?sys WHERE { {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;}  {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}BIND(<http://semoss.org/ontologies/Relation/Consume> AS ?downstream) {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?icd ?downstream ?sys ;} {?payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload> } {?icd ?payload ?data ;} FILTER(STR(?sys)!=\"http://health.mil/ontologies/Concept/System/DHMSM\")}";
	
	/**
	 * Constructor for GLItemGeneratorSelfReportedFutureInterfaces.
	 */
	public GLItemGeneratorSelfReportedFutureInterfaces(IEngine hrCore, IEngine futureState, IEngine futureCostDB, IEngine costDB) {
		this.hrCore = hrCore;
		this.futureDB = futureState;
		this.futureCostDB = futureCostDB;
		this.tapCost = costDB;
	}

	public void genData(){
		GLItemGeneratorICDValidated generator = new GLItemGeneratorICDValidated();
		prepareGLItemGenerator(generator);
		runGenerator(generator);
		Hashtable<String, Vector<String[]>> allData = generator.getAllDataHash();

		Gson gson = new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create();
		System.err.println(gson.toJson(allData));
//		getData();
//		insertData();
	}
	
	private void prepareGLItemGenerator(GLItemGeneratorICDValidated generator){
		generator.setCoreEngine(hrCore);
		generator.fillSystemComplexityHash();
		
		generator.setCoreEngine(futureDB);
		generator.genSDLCVector();
		generator.prepareAllDataHash();
		generator.setGenSpecificDProtQuery(this.genSpecificDProtQuery);
		generator.setGenSpecificDFormQuery(this.genSpecificDFormQuery);
		generator.setProviderDataQuery1(this.providerDataQuery1);
		generator.setProviderDataQuery2(this.providerDataQuery2);
		generator.setConsumerDataQuery(this.consumerDataQuery);

	}
	
	private void runGenerator(GLItemGeneratorICDValidated generator){
		generator.fillProviderDataList();
		generator.fillConsumerDataList();
		
		generator.genSpecificProtocol();
		generator.genProviderDataList();
		generator.genConsumerDataList();
		generator.genCoreTasks();
		generator.genFactors();
	}
	
}
