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

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.poi.main.POIWriter;
import prerna.poi.main.RelationshipLoadingSheetWriter;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * This class is used to generate items in the GL and is used to update the cost database.
 */
public class GLItemGeneratorICDValidated {

	public Hashtable<String, Vector<String[]>> allDataHash = new Hashtable<String, Vector<String[]>>();
	public Hashtable<String, Vector<String[]>> genericDProtHash = new Hashtable<String, Vector<String[]>>();
	public Hashtable<String, Vector<String[]>> genericDFormHash = new Hashtable<String, Vector<String[]>>();
	public Hashtable<String, Vector<String>> specificDProtHash = new Hashtable<String, Vector<String>>();
	public Hashtable<String, Vector<String>> specificDFormHash = new Hashtable<String, Vector<String>>();
	public Vector<String> sdlcV = new Vector<String>();
	ArrayList <String[]> genericDataList = new ArrayList<String[]>();
	ArrayList <String[]> genericBLUList	= new ArrayList<String[]>();
	ArrayList <String[]> providerDataList = new ArrayList<String[]>();
	ArrayList <String[]> providerBLUList = new ArrayList<String[]>();
	ArrayList <String[]> consumerList = new ArrayList<String[]>();
	ArrayList <String[]> coreTaskList = new ArrayList<String[]>();
	ArrayList <String[]> subTaskList = new ArrayList<String[]>();
	Hashtable<String, String> sysCompHash = new Hashtable<String, String>();
	private IEngine coreEngine;
	private String genSpecificDProtQuery = "SELECT DISTINCT ?ser ?data ?sys (COALESCE(?dprot, (URI(\"http://health.mil/ontologies/Concept/DProt/HTTPS-SOAP\"))) AS ?Prot) WHERE { {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;}  BIND(<http://semoss.org/ontologies/Relation/Exposes> AS ?exposes) {?ser ?exposes ?data ;} {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}BIND(<http://semoss.org/ontologies/Relation/Consume> AS ?downstream) {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?icd ?downstream ?sys ;} BIND(<http://semoss.org/ontologies/Relation/Payload> AS ?payload) {?icd ?payload ?data ;} OPTIONAL{ BIND(<http://semoss.org/ontologies/Relation/Has> AS ?has) {?dprot <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DProt> ;} {?icd ?has ?dprot ;}} }";
	private String genSpecificDFormQuery = "SELECT DISTINCT ?ser ?data ?sys (COALESCE(?dform, (URI(\"http://health.mil/ontologies/Concept/DForm/XML\"))) AS ?Form) WHERE { {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;}  BIND(<http://semoss.org/ontologies/Relation/Exposes> AS ?exposes) {?ser ?exposes ?data ;} {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} BIND(<http://semoss.org/ontologies/Relation/Consume> AS ?downstream) {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?icd ?downstream ?sys ;} BIND(<http://semoss.org/ontologies/Relation/Payload> AS ?payload) {?icd ?payload ?data ;} OPTIONAL{ BIND(<http://semoss.org/ontologies/Relation/Has> AS ?has) {?dform <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DForm> ;} {?icd ?has ?dform ;}} }";
	private String providerDataQuery1 = "SELECT DISTINCT ?ser ?data ?sys WHERE { {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?exposes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Exposes>;} {?ser ?exposes ?data ;} {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?sys ?provide ?data ;} {?provide <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm ;} {?upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?sys ?upstream ?icd ;} {?payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?icd ?payload ?data ;} } BINDINGS ?crm {(\"C\")(\"M\")}";
	private String providerDataQuery2 = "SELECT DISTINCT ?Service ?Data ?System WHERE { {?Service <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;} {?otherSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} OPTIONAL{{?icd2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?icd2 <http://semoss.org/ontologies/Relation/Consume> ?System} {?icd2 <http://semoss.org/ontologies/Relation/Payload> ?Data}} {?Service <http://semoss.org/ontologies/Relation/Exposes> ?Data} {?System <http://semoss.org/ontologies/Relation/Provide> ?icd ;} {?icd <http://semoss.org/ontologies/Relation/Consume> ?otherSystem ;} {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data ;} FILTER(!BOUND(?icd2)) } ORDER BY ?System";
	private String consumerDataQuery = "SELECT DISTINCT ?ser ?data ?sys WHERE { {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?exposes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Exposes>;} {?ser ?exposes ?data ;} {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?sys ?provide ?data ;} {?downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?icd ?downstream ?sys ;} {?payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?icd ?payload ?data ;} }";

	/**
	 */
	public enum CHANGED_DB {CORE, SITE};

	/**
	 * Constructor for GLItemGeneratorICDValidated.
	 */
	public GLItemGeneratorICDValidated() {
	
	}

	/**
	 * /**
	 * Generates the list of GL items.
	 * @param changedDB CHANGED_DB
	 */
	public void genList(CHANGED_DB changedDB)
	{
		if(changedDB == CHANGED_DB.CORE) {
			runAllQuery();
			genSDLCVector();
			prepareAllDataHash();
			genGenericProtocolCount();
			genSpecificProtocol();
			genGenericDataList();
			genGenericBLUList();
			genProviderDataList();
			genProviderBLUList();
			genConsumerDataList();
			genCoreTasks();
			genFactors();
			exportCoreRelationships();

			POIWriter poiwrite = new POIWriter();
			poiwrite.runExport(allDataHash, Constants.GLITEM_CORE_LOADING_SHEET, null, true);
		}
		else if(changedDB == CHANGED_DB.SITE) {
			exportSiteRelationships();
			POIWriter poiwrite = new POIWriter();
			poiwrite.runExport(allDataHash, Constants.GLITEM_SITE_LOADING_SHEET, null, true);
		}
	}

	/**
	 * Generates factors for the all data hashtable (subtask properties and CT/ST).
	 */
	public void genFactors() {
		Vector<String[]> subV = allDataHash.get("GLItemCT-GLItemST");
		Vector<String[]> newV = allDataHash.get("GLItemSubTaskProp");
		//starting at 2 because tthe first array has bs taht Bill added
		for (int i = 2; i < subV.size();i++)
		{
			String[] rowArray= subV.get(i);
			String[] newArray = new String[2];
			newArray[0]=rowArray[1];
			newArray[1]="1";
			newV.addElement(newArray);
		}
		allDataHash.put("GLItemSubTaskProp", newV);
	}

	/**
	 * Generates core tasks taking into account generic, consumer, or data federation types.
	 */
	public void genCoreTasks()
	{
		Vector<String[]> glItemV;
		for (int sdlcIdx=0 ; sdlcIdx<sdlcV.size();sdlcIdx++)
		{
			glItemV = allDataHash.get("Data-"+sdlcV.get(sdlcIdx)+"GLItem");
			for (int i=0; i<glItemV.size(); i++)
			{
				String[] vecRet = (String[]) glItemV.get(i);
				//have to do it in this order because of the data
				if (vecRet[1].contains("Generic"))
				{
					processDataGenericGL(vecRet[1], (String) sdlcV.get(sdlcIdx));
				}
				else if (vecRet[1].contains("Consumer"))
				{
					processConsumerGL(vecRet[1], (String) sdlcV.get(sdlcIdx));
				}
				else if (vecRet[1].contains("Data Federation"))
				{
					processDataProviderGL(vecRet[1], (String) sdlcV.get(sdlcIdx));
				}
			}
			glItemV = allDataHash.get("BLU-"+sdlcV.get(sdlcIdx)+"GLItem");
			for (int i=0; i<glItemV.size(); i++)
			{
				String[] vecRet = (String[]) glItemV.get(i);
				//have to do it in this order because of the data
				if (vecRet[1].contains("Generic"))
				{
					processBLUGenericGL(vecRet[1], (String) sdlcV.get(sdlcIdx));
				}
				else if (vecRet[1].contains("Provider"))
				{
					processBLUProviderGL(vecRet[1], (String) sdlcV.get(sdlcIdx));
				}
			}
		}
	}

	/**
	 * Splits a given string from the GL and processes the generic data GL items given a specific phase.
	 * @param glString String
	 * @param phase String
	 */
	public void processDataGenericGL(String glString, String phase)
	{
		Vector<String[]> coreTaskV = allDataHash.get(phase+"GLItem-GLItemCT");
		Vector<String[]> coreTaskBasisV = allDataHash.get("GLItemCT-BasisCT");
		Vector<String[]> subTaskV = allDataHash.get("GLItemCT-GLItemST");
		Vector<String[]> subTaskCompV = allDataHash.get("GLItemST-STBasisCompComp");

		String[] inputArray = new String[2];
		inputArray[0] = glString;
		inputArray[1] = glString+"%Data Protocol Facade";
		coreTaskV.addElement(inputArray);

		String basisCoreTask = getBasisCoreTask(inputArray[1]);
		inputArray = new String[2];
		inputArray[0] = glString+"%Data Protocol Facade";
		inputArray[1] = basisCoreTask;
		coreTaskBasisV.addElement(inputArray);

		inputArray = new String[2];
		inputArray[0] = glString;
		inputArray[1] = glString+"%Wire Protocol Facade";
		coreTaskV.addElement(inputArray);

		basisCoreTask = getBasisCoreTask(inputArray[1]);
		inputArray = new String[2];
		inputArray[0] = glString+"%Wire Protocol Facade";
		inputArray[1] = basisCoreTask;
		coreTaskBasisV.addElement(inputArray);

		inputArray = new String[2];
		inputArray[0] = glString;
		inputArray[1] = glString+"%Security";
		coreTaskV.addElement(inputArray);

		basisCoreTask = getBasisCoreTask(inputArray[1]);
		inputArray = new String[2];
		inputArray[0] = glString+"%Security";
		inputArray[1] = basisCoreTask;
		coreTaskBasisV.addElement(inputArray);

		//security subtask determined already
		inputArray = new String[2];
		inputArray[0] = glString+"%Security";
		inputArray[1] = glString+"%Security";
		subTaskV.addElement(inputArray);

		//complexity always simple
		String basisSubTask = getBasisCoreTask(inputArray[1]);
		inputArray = new String[2];
		inputArray[0] = glString+"%Security";
		inputArray[1] = basisSubTask +"%Simple%Simple";
		subTaskCompV.addElement(inputArray);

		allDataHash.put(phase+"GLItem-GLItemCT", coreTaskV);
		allDataHash.put("GLItemCT-BasisCT", coreTaskBasisV);

		Vector<String[]> dFormV = genericDFormHash.get(glString);
		if (dFormV == null)
		{
			dFormV = new Vector<String[]>();
		}
		for (int i=0; i < dFormV.size(); i++)
		{
			inputArray = new String[2];
			String[] vecArray= dFormV.get(i);
			Double protCountDouble = Double.parseDouble(vecArray[1]);
			int protCount = (int) Math.round(protCountDouble);
			String complexity = "";
			inputArray[0] = glString+"%Data Protocol Facade";
			inputArray[1] = glString+"%"+vecArray[0];
			subTaskV.addElement(inputArray);
			if (protCount < 50)
			{
				complexity = "Simple";
			}
			else if (protCount >=50 && protCount <100)
			{
				complexity = "Medium";
			}
			else if (protCount >= 100)
			{
				complexity = "Complex";
			}
			basisSubTask = getBasisCoreTask(inputArray[1]);
			inputArray = new String[2];
			inputArray[0] = glString+"%"+vecArray[0];;
			inputArray[1] = basisSubTask+"%"+complexity+"%Simple";
			subTaskCompV.addElement(inputArray);
		}

		Vector<String[]> dProtV = genericDProtHash.get(glString);
		if (dProtV == null)
		{
			dProtV = new Vector<String[]>();
		}
		for (int i=0; i < dProtV.size(); i++)
		{
			inputArray = new String[2];
			String[] vecArray= dProtV.get(i);
			Double protCountDouble = Double.parseDouble(vecArray[1]);
			int protCount = (int) Math.round(protCountDouble);
			String complexity = "";
			inputArray[0] = glString+"%Wire Protocol Facade";
			inputArray[1] = glString+"%"+vecArray[0];
			subTaskV.addElement(inputArray);
			if (protCount < 50)
			{
				complexity = "Simple";
			}
			else if (protCount >=50 && protCount <100)
			{
				complexity = "Medium";
			}
			else if (protCount >= 100)
			{
				complexity = "Complex";
			}
			basisSubTask = getBasisCoreTask(inputArray[1]);
			inputArray = new String[2];
			inputArray[0] = glString+"%"+vecArray[0];;
			inputArray[1] = basisSubTask+"%"+complexity+"%Simple";
			subTaskCompV.addElement(inputArray);
		}
		allDataHash.put("GLItemCT-GLItemST", subTaskV);
		allDataHash.put("GLItemST-STBasisCompComp", subTaskCompV);
	}

	/**
	 * Splits a given string from the GL and processes the provider data GL items given a specific phase.
	 * @param glString String
	 * @param phase String
	 */
	public void processDataProviderGL(String glString, String phase)
	{
		Vector<String[]> coreTaskV = allDataHash.get(phase+"GLItem-GLItemCT");
		Vector<String[]> coreTaskBasisV = allDataHash.get("GLItemCT-BasisCT");
		Vector<String[]> subTaskV = allDataHash.get("GLItemCT-GLItemST");
		Vector<String[]> subTaskCompV = allDataHash.get("GLItemST-STBasisCompComp");

		String[] inputArray = new String[2];
		inputArray[0] = glString;
		inputArray[1] = glString+"%Horizontal Federation";
		coreTaskV.addElement(inputArray);
		String basisCoreTask = getBasisCoreTask(inputArray[1]);

		inputArray = new String[2];
		inputArray[0] = glString+"%Horizontal Federation";
		inputArray[1] = basisCoreTask;
		coreTaskBasisV.addElement(inputArray);

		allDataHash.put(phase+"GLItem-GLItemCT", coreTaskV);

		inputArray = new String[2];
		inputArray[0] = glString+"%Horizontal Federation";
		inputArray[1] = glString+"%Instrument querying through RDF";
		subTaskV.addElement(inputArray);
		allDataHash.put("GLItemCT-GLItemST", subTaskV);

		String[] split = glString.split("%");
		String complexity = sysCompHash.get(split[2]);
		if (complexity ==null)
			complexity = "Simple";

		String basisSubTask = getBasisCoreTask(inputArray[1]);
		inputArray = new String[2];
		inputArray[0] = glString+"%Instrument querying through RDF";
		inputArray[1] = basisSubTask+"%"+complexity+"%"+complexity;
		subTaskCompV.addElement(inputArray);
		allDataHash.put("GLItemST-STBasisCompComp", subTaskCompV);
	}

	/**
	 * Splits a given string from the GL and processes the generic BLU GL items given a specific phase.
	 * @param glString String
	 * @param phase String
	 */
	public void processBLUGenericGL(String glString,  String phase)
	{
		Vector<String[]> coreTaskV = allDataHash.get(phase+"GLItem-GLItemCT");
		Vector<String[]> coreTaskBasisV = allDataHash.get("GLItemCT-BasisCT");
		Vector<String[]> subTaskV = allDataHash.get("GLItemCT-GLItemST");
		Vector<String[]> subTaskCompV = allDataHash.get("GLItemST-STBasisCompComp");

		String[] inputArray = new String[2];
		inputArray[0] = glString;
		inputArray[1] = glString+"%Data Protocol Facade";
		coreTaskV.addElement(inputArray);

		String basisCoreTask = getBasisCoreTask(inputArray[1]);
		inputArray = new String[2];
		inputArray[0] = glString+"%Data Protocol Facade";
		inputArray[1] = basisCoreTask;
		coreTaskBasisV.addElement(inputArray);

		inputArray = new String[2];
		inputArray[0] = glString;
		inputArray[1] = glString+"%Wire Protocol Facade";
		coreTaskV.addElement(inputArray);

		basisCoreTask = getBasisCoreTask(inputArray[1]);
		inputArray = new String[2];
		inputArray[0] = glString+"%Wire Protocol Facade";
		inputArray[1] = basisCoreTask;
		coreTaskBasisV.addElement(inputArray);

		inputArray = new String[2];
		inputArray[0] = glString;
		inputArray[1] = glString+"%Security";
		coreTaskV.addElement(inputArray);

		basisCoreTask = getBasisCoreTask(inputArray[1]);
		inputArray = new String[2];
		inputArray[0] = glString+"%Security";
		inputArray[1] = basisCoreTask;
		coreTaskBasisV.addElement(inputArray);

		//security subtask determined already
		inputArray = new String[2];
		inputArray[0] = glString+"%Security";
		inputArray[1] = glString+"%Security";
		subTaskV.addElement(inputArray);

		//complexity always simple
		String basisSubTask = getBasisCoreTask(inputArray[1]);
		inputArray = new String[2];
		inputArray[0] = glString+"%Security";
		inputArray[1] = basisSubTask+"%Simple%Simple";
		subTaskCompV.addElement(inputArray);

		inputArray = new String[2];
		inputArray[0] = glString;
		inputArray[1] = glString+"%Logging and Audit Trail";
		coreTaskV.addElement(inputArray);

		basisCoreTask = getBasisCoreTask(inputArray[1]);
		inputArray = new String[2];
		inputArray[0] = glString+"%Logging and Audit Trail";
		inputArray[1] = basisCoreTask;
		coreTaskBasisV.addElement(inputArray);

		//security subtask determined already
		inputArray = new String[2];
		inputArray[0] = glString+"%Logging and Audit Trail";
		inputArray[1] = glString+"%Logging and Audit Trail";
		subTaskV.addElement(inputArray);

		//complexity always simple
		basisSubTask = getBasisCoreTask(inputArray[1]);
		inputArray = new String[2];
		inputArray[0] = glString+"%Logging and Audit Trail";
		inputArray[1] = basisSubTask+"%Simple%Simple";
		subTaskCompV.addElement(inputArray);

		//BLU data and wire protocol items are predetermined
		inputArray = new String[2];
		inputArray[0] = glString+"%Data Protocol Facade";
		inputArray[1] = glString+"%XML";
		subTaskV.addElement(inputArray);

		basisSubTask = getBasisCoreTask(inputArray[1]);
		inputArray = new String[2];
		inputArray[0] = glString+"%XML";
		inputArray[1] = basisSubTask+"%Medium%Simple";
		subTaskCompV.addElement(inputArray);

		inputArray = new String[2];
		inputArray[0] = glString+"%Wire Protocol Facade";
		inputArray[1] = glString+"%HTTPS/SOAP";
		subTaskV.addElement(inputArray);

		basisSubTask = getBasisCoreTask(inputArray[1]);
		inputArray = new String[2];
		inputArray[0] = glString+"%HTTPS/SOAP";
		inputArray[1] = basisSubTask+"%Simple%Simple";
		subTaskCompV.addElement(inputArray);

		allDataHash.put(phase+"GLItem-GLItemCT", coreTaskV);
		allDataHash.put("GLItemCT-BasisCT", coreTaskBasisV);
		allDataHash.put("GLItemCT-GLItemST", subTaskV);
		allDataHash.put("GLItemST-STBasisCompComp", subTaskCompV);
	}

	/**
	 * Splits a given string from the GL and processes the provider BLU GL items given a specific phase.
	 * @param glString String
	 * @param phase String
	 */
	public void processBLUProviderGL(String glString, String phase)
	{
		Vector<String[]> coreTaskV = allDataHash.get(phase+"GLItem-GLItemCT");
		Vector<String[]> coreTaskBasisV = allDataHash.get("GLItemCT-BasisCT");
		Vector<String[]> subTaskV = allDataHash.get("GLItemCT-GLItemST");
		Vector<String[]> subTaskCompV = allDataHash.get("GLItemST-STBasisCompComp");

		String[] inputArray = new String[2];
		inputArray[0] = glString;
		inputArray[1] = glString+"%Service Business Logic";
		coreTaskV.addElement(inputArray);
		String basisCoreTask = getBasisCoreTask(inputArray[1]);

		inputArray = new String[2];
		inputArray[0] = glString+"%Service Business Logic";
		inputArray[1] = basisCoreTask;
		coreTaskBasisV.addElement(inputArray);

		allDataHash.put(phase+"GLItem-GLItemCT", coreTaskV);

		inputArray = new String[2];
		inputArray[0] = glString+"%Service Business Logic";
		inputArray[1] = glString+"%Service Business Logic";
		subTaskV.addElement(inputArray);
		allDataHash.put("GLItemCT-GLItemST", subTaskV);

		String[] split = glString.split("%");
		String complexity = sysCompHash.get(split[2]);
		if (complexity ==null)
			complexity = "Simple";

		String basisSubTask = getBasisCoreTask(inputArray[1]);		
		inputArray = new String[2];
		inputArray[0] = glString+"%Service Business Logic";
		inputArray[1] = basisSubTask+"%"+complexity+"%"+complexity;
		subTaskCompV.addElement(inputArray);
		allDataHash.put("GLItemST-STBasisCompComp", subTaskCompV);
	}

	/**
	 * Splits a given string from the GL and processes the consumer GL items given a specific phase.
	 * Determines the security subtask and factors in system complexity.
	 * @param glString String
	 * @param phase String
	 */
	public void processConsumerGL(String glString, String phase)
	{
		Vector<String[]> coreTaskV = allDataHash.get(phase+"GLItem-GLItemCT");
		Vector<String[]> coreTaskBasisV = allDataHash.get("GLItemCT-BasisCT");
		Vector<String[]> subTaskV = allDataHash.get("GLItemCT-GLItemST");
		Vector<String[]> subTaskCompV = allDataHash.get("GLItemST-STBasisCompComp");
		String[] inputArray = new String[2];

		//system complexity piece
		String[] split = glString.split("%");
		String complexity = sysCompHash.get(split[2]);
		if (complexity ==null)
			complexity = "Simple";

		inputArray = new String[2];
		inputArray[0] = glString;
		inputArray[1] = glString+"%Security";
		coreTaskV.addElement(inputArray);

		String basisCoreTask = getBasisCoreTask(inputArray[1]);
		inputArray = new String[2];
		inputArray[0] = glString+"%Security";
		inputArray[1] = basisCoreTask;
		coreTaskBasisV.addElement(inputArray);

		//security subtask determined already
		inputArray = new String[2];
		inputArray[0] = glString+"%Security";
		inputArray[1] = glString+"%Security";
		subTaskV.addElement(inputArray);

		//complexity always simple
		String basisSubTask = getBasisCoreTask(inputArray[1]);
		inputArray = new String[2];
		inputArray[0] = glString+"%Security";
		inputArray[1] = basisSubTask+"%Simple%"+complexity;
		subTaskCompV.addElement(inputArray);

		Vector<String> dProtV = specificDProtHash.get(glString);
		if (dProtV == null)
		{
			dProtV = new Vector<String>();
		}
		for (int i=0;i<dProtV.size();i++)
		{
			inputArray = new String[2];
			inputArray[0] = glString;
			inputArray[1] = glString+"%Wire Protocol Facade";
			coreTaskV.addElement(inputArray);

			basisCoreTask = getBasisCoreTask(inputArray[1]);
			inputArray = new String[2];
			inputArray[0] = glString+"%Wire Protocol Facade";
			inputArray[1] = basisCoreTask;
			coreTaskBasisV.addElement(inputArray);

			inputArray = new String[2];
			inputArray[0] = glString+"%Wire Protocol Facade";
			inputArray[1] = glString+"%"+dProtV.get(i);
			subTaskV.addElement(inputArray);

			basisSubTask = getBasisCoreTask(inputArray[1]);		
			inputArray = new String[2];
			inputArray[0] = glString+"%"+dProtV.get(i);
			inputArray[1] = basisSubTask+"%Simple%"+complexity;
			subTaskCompV.addElement(inputArray);
		}

		Vector<String> dFormV = specificDFormHash.get(glString);
		if (dFormV == null)
		{
			dFormV = new Vector<String>();
		}
		for (int i=0;i<dFormV.size();i++)
		{
			inputArray = new String[2];
			inputArray[0] = glString;
			inputArray[1] = glString+"%Data Protocol Facade";
			coreTaskV.addElement(inputArray);

			basisCoreTask = getBasisCoreTask(inputArray[1]);
			inputArray = new String[2];
			inputArray[0] = glString+"%Data Protocol Facade";
			inputArray[1] = basisCoreTask;
			coreTaskBasisV.addElement(inputArray);

			inputArray = new String[2];
			inputArray[0] = glString+"%Data Protocol Facade";
			inputArray[1] = glString+"%"+dFormV.get(i);
			subTaskV.addElement(inputArray);

			//system complexity comes second in this case

			basisSubTask = getBasisCoreTask(inputArray[1]);		
			inputArray = new String[2];
			inputArray[0] = glString+"%"+dFormV.get(i);
			inputArray[1] = basisSubTask+"%Simple%"+complexity;
			subTaskCompV.addElement(inputArray);
		}
		allDataHash.put(phase+"GLItem-GLItemCT", coreTaskV);
		allDataHash.put("GLItemCT-BasisCT", coreTaskBasisV);
		allDataHash.put("GLItemCT-GLItemST", subTaskV);
		allDataHash.put("GLItemST-STBasisCompComp", subTaskCompV);
	}

	/**
	 * Splits a string to obtain the GL basis core task.
	 * @param glCoreTask String to be split.
	 * @return String	Basis core task. 
	 */
	public String getBasisCoreTask (String glCoreTask)
	{
		String[] split = glCoreTask.split("%");
		String basisCoreTask = split[3]+"%"+split[4]+"%"+split[5];
		return basisCoreTask;
	}

	/**
	 * Generates a list of generic data.
	 */
	public void genGenericDataList()
	{
		ArrayList <String[]> retList = genericDataList;

		//go through all SDLC
		for (int sdlcIdx=0 ; sdlcIdx<sdlcV.size();sdlcIdx++)
		{
			Vector<String[]> inputTagV = allDataHash.get(sdlcV.get(sdlcIdx)+"GLItemTag");
			Vector<String[]> inputSerV = allDataHash.get(sdlcV.get(sdlcIdx)+"GLItem-Ser");
			Vector<String[]> inputPhaseV = allDataHash.get(sdlcV.get(sdlcIdx)+"GLItem-Phase");
			Vector<String[]> inputDataV = allDataHash.get("Data-"+sdlcV.get(sdlcIdx)+"GLItem");

			for (int i=0;i<retList.size();i++)
			{
				String[] retLine = retList.get(i);
				String dataStr = retLine[1];
				String serStr = retLine[0];
				String sysStr = "Generic";
				String[] inputLine = new String[2];
				//create rows for glitem-tag sheets first
				inputLine[0] = dataStr + "%" + serStr + "%" + sysStr + "%"+"Provider"+"%"+sdlcV.get(sdlcIdx);
				inputLine[1] = "Generic";
				inputTagV.addElement(inputLine);

				//create rows for glitem-ser, only have to chnage the 2nd column
				inputLine = new String[2];
				inputLine[0] = dataStr + "%" + serStr + "%" + sysStr + "%"+"Provider"+"%"+sdlcV.get(sdlcIdx);
				inputLine[1] = serStr;
				inputSerV.addElement(inputLine);

				//create rows for glitem-phase, only have to change 2nd column
				inputLine = new String[2];
				inputLine[0] = dataStr + "%" + serStr + "%" + sysStr + "%"+"Provider"+"%"+sdlcV.get(sdlcIdx);
				inputLine[1] = (String) sdlcV.get(sdlcIdx);
				inputPhaseV.addElement(inputLine);

				//create rows for data-glitem, only have to change 2nd column
				inputLine = new String[2];
				inputLine[0] = dataStr;
				inputLine[1] = dataStr + "%" + serStr + "%" + sysStr + "%"+"Provider"+"%"+sdlcV.get(sdlcIdx);
				inputDataV.addElement(inputLine);
			}
			allDataHash.put(sdlcV.get(sdlcIdx)+"GLItemTag", inputTagV);
			allDataHash.put(sdlcV.get(sdlcIdx)+"GLItem-Ser", inputSerV);
			allDataHash.put(sdlcV.get(sdlcIdx)+"GLItem-Phase", inputPhaseV);
			allDataHash.put("Data-"+sdlcV.get(sdlcIdx)+"GLItem", inputDataV);
		}
	}

	/**
	 * Generates a list of generic BLUs.
	 */
	public void genGenericBLUList()
	{
		ArrayList <String[]> retList = genericBLUList;

		//go through all SDLC
		for (int sdlcIdx=0 ; sdlcIdx<sdlcV.size();sdlcIdx++)
		{
			Vector<String[]> inputTagV = allDataHash.get(sdlcV.get(sdlcIdx)+"GLItemTag");
			Vector<String[]> inputSerV = allDataHash.get(sdlcV.get(sdlcIdx)+"GLItem-Ser");
			Vector<String[]> inputPhaseV = allDataHash.get(sdlcV.get(sdlcIdx)+"GLItem-Phase");
			Vector<String[]> inputBLUV = allDataHash.get("BLU-"+sdlcV.get(sdlcIdx)+"GLItem");

			for (int i=0;i<retList.size();i++)
			{
				String[] retLine = retList.get(i);
				String dataStr = retLine[1];
				String serStr = retLine[0];
				String sysStr = "Generic";
				String[] inputLine = new String[2];
				//create rows for glitem-tag sheets first
				inputLine[0] = dataStr + "%" + serStr + "%" + sysStr + "%"+"Provider"+"%"+sdlcV.get(sdlcIdx);
				inputLine[1] = "Generic";
				inputTagV.addElement(inputLine);

				//create rows for glitem-ser, only have to chnage the 2nd column
				inputLine = new String[2];
				inputLine[0] = dataStr + "%" + serStr + "%" + sysStr + "%"+"Provider"+"%"+sdlcV.get(sdlcIdx);
				inputLine[1] = serStr;
				inputSerV.addElement(inputLine);

				//create rows for glitem-phase, only have to change 2nd column
				inputLine = new String[2];
				inputLine[0] = dataStr + "%" + serStr + "%" + sysStr + "%"+"Provider"+"%"+sdlcV.get(sdlcIdx);
				inputLine[1] = (String) sdlcV.get(sdlcIdx);
				inputPhaseV.addElement(inputLine);

				//create rows for data-glitem, only have to change 2nd column
				inputLine = new String[2];
				inputLine[0] = dataStr;
				inputLine[1] = dataStr + "%" + serStr + "%" + sysStr + "%"+"Provider"+"%"+sdlcV.get(sdlcIdx);
				inputBLUV.addElement(inputLine);
			}
			allDataHash.put(sdlcV.get(sdlcIdx)+"GLItemTag", inputTagV);
			allDataHash.put(sdlcV.get(sdlcIdx)+"GLItem-Ser", inputSerV);
			allDataHash.put(sdlcV.get(sdlcIdx)+"GLItem-Phase", inputPhaseV);
			allDataHash.put("BLU-"+sdlcV.get(sdlcIdx)+"GLItem", inputBLUV);
		}
	}

	/**
	 * Generates a list of data for providers.
	 */
	public void genProviderDataList()
	{
		ArrayList <String[]> retList = providerDataList;

		//go through all SDLC
		for (int sdlcIdx=0 ; sdlcIdx<sdlcV.size()-1;sdlcIdx++)
		{
			Vector<String[]> inputTagV = allDataHash.get(sdlcV.get(sdlcIdx)+"GLItemTag");
			Vector<String[]> inputSerV = allDataHash.get(sdlcV.get(sdlcIdx)+"GLItem-Ser");
			Vector<String[]> inputPhaseV = allDataHash.get(sdlcV.get(sdlcIdx)+"GLItem-Phase");
			Vector<String[]> inputSysV = allDataHash.get("Sys-"+sdlcV.get(sdlcIdx)+"GLItem");
			Vector<String[]> inputDataV = allDataHash.get("Data-"+sdlcV.get(sdlcIdx)+"GLItem");

			//not going through all SDLC because datafed deploy does not exist
			for (int i = 0; i < retList.size(); i++)
			{
				String[] retLine = retList.get(i);
				String dataStr = retLine[1];
				String serStr = retLine[0];
				String sysStr = retLine[2];
				String[] inputLine = new String[2];
				//create rows for glitem-tag sheets first
				inputLine[0] = dataStr + "%" + serStr + "%" + sysStr + "%"+"Data Federation"+"%"+sdlcV.get(sdlcIdx);
				inputLine[1] = "Provider";
				inputTagV.addElement(inputLine);

				//create rows for glitem-ser, only have to chnage the 2nd column
				inputLine = new String[2];
				inputLine[0] = dataStr + "%" + serStr + "%" + sysStr + "%"+"Data Federation"+"%"+sdlcV.get(sdlcIdx);
				inputLine[1] = serStr;
				inputSerV.addElement(inputLine);

				//create rows for glitem-phase, only have to change 2nd column
				inputLine = new String[2];
				inputLine[0] = dataStr + "%" + serStr + "%" + sysStr + "%"+"Data Federation"+"%"+sdlcV.get(sdlcIdx);
				inputLine[1] = (String) sdlcV.get(sdlcIdx);
				inputPhaseV.addElement(inputLine);

				//create rows for sys-glitem, only have to change 2nd column
				inputLine = new String[2];
				inputLine[0] = sysStr;
				inputLine[1] = dataStr + "%" + serStr + "%" + sysStr + "%"+"Data Federation"+"%"+sdlcV.get(sdlcIdx);
				inputSysV.addElement(inputLine);

				//create rows for data-glitem, only have to change 2nd column
				inputLine = new String[2];
				inputLine[0] = dataStr;
				inputLine[1] = dataStr + "%" + serStr + "%" + sysStr + "%"+"Data Federation"+"%"+sdlcV.get(sdlcIdx);
				inputDataV.addElement(inputLine);
			}
			allDataHash.put(sdlcV.get(sdlcIdx)+"GLItemTag", inputTagV);
			allDataHash.put(sdlcV.get(sdlcIdx)+"GLItem-Ser", inputSerV);
			allDataHash.put(sdlcV.get(sdlcIdx)+"GLItem-Phase", inputPhaseV);
			allDataHash.put("Sys-"+sdlcV.get(sdlcIdx)+"GLItem", inputSysV);
			allDataHash.put("Data-"+sdlcV.get(sdlcIdx)+"GLItem", inputDataV);
		}
	}

	/**
	 * Generates a list of BLUs for providers.
	 */
	public void genProviderBLUList()
	{
		ArrayList <String[]> retList = providerBLUList;

		//go through all SDLC
		for (int sdlcIdx=0 ; sdlcIdx<sdlcV.size()-1;sdlcIdx++)
		{
			Vector<String[]> inputTagV = allDataHash.get(sdlcV.get(sdlcIdx)+"GLItemTag");
			Vector<String[]> inputSerV = allDataHash.get(sdlcV.get(sdlcIdx)+"GLItem-Ser");
			Vector<String[]> inputPhaseV = allDataHash.get(sdlcV.get(sdlcIdx)+"GLItem-Phase");
			Vector<String[]> inputSysV = allDataHash.get("Sys-"+sdlcV.get(sdlcIdx)+"GLItem");
			Vector<String[]> inputBLUV = allDataHash.get("BLU-"+sdlcV.get(sdlcIdx)+"GLItem");

			for (int i=0;i<retList.size();i++)
			{
				String[] retLine = retList.get(i);
				String dataStr = retLine[1];
				String serStr = retLine[0];
				String sysStr = retLine[2];
				String[] inputLine = new String[2];
				//create rows for glitem-tag sheets first
				inputLine[0] = dataStr + "%" + serStr + "%" + sysStr + "%"+"Provider"+"%"+sdlcV.get(sdlcIdx);
				inputLine[1] = "Provider";
				inputTagV.addElement(inputLine);

				//create rows for glitem-ser, only have to chnage the 2nd column
				inputLine = new String[2];
				inputLine[0] = dataStr + "%" + serStr + "%" + sysStr + "%"+"Provider"+"%"+sdlcV.get(sdlcIdx);
				inputLine[1] = serStr;
				inputSerV.addElement(inputLine);

				//create rows for glitem-phase, only have to change 2nd column
				inputLine = new String[2];
				inputLine[0] = dataStr + "%" + serStr + "%" + sysStr + "%"+"Provider"+"%"+sdlcV.get(sdlcIdx);
				inputLine[1] = (String) sdlcV.get(sdlcIdx);
				inputPhaseV.addElement(inputLine);

				//create rows for sys-glitem, only have to change 2nd column
				inputLine = new String[2];
				inputLine[0] = sysStr;
				inputLine[1] = dataStr + "%" + serStr + "%" + sysStr + "%"+"Provider"+"%"+sdlcV.get(sdlcIdx);
				inputSysV.addElement(inputLine);

				//create rows for data-glitem, only have to change 2nd column
				inputLine = new String[2];
				inputLine[0] = dataStr;
				inputLine[1] = dataStr + "%" + serStr + "%" + sysStr + "%"+"Provider"+"%"+sdlcV.get(sdlcIdx);
				inputBLUV.addElement(inputLine);
			}
			allDataHash.put(sdlcV.get(sdlcIdx)+"GLItemTag", inputTagV);
			allDataHash.put(sdlcV.get(sdlcIdx)+"GLItem-Ser", inputSerV);
			allDataHash.put(sdlcV.get(sdlcIdx)+"GLItem-Phase", inputPhaseV);
			allDataHash.put("Sys-"+sdlcV.get(sdlcIdx)+"GLItem", inputSysV);
			allDataHash.put("BLU-"+sdlcV.get(sdlcIdx)+"GLItem", inputBLUV);
		}
	}

	/**
	 * Generates a list of data for GL items relating to consumers.
	 */
	public void genConsumerDataList()
	{
		ArrayList <String[]> retList = consumerList;

		//go through all SDLC
		for (int sdlcIdx=0 ; sdlcIdx<sdlcV.size()-1;sdlcIdx++)
		{
			Vector<String[]> inputTagV = allDataHash.get(sdlcV.get(sdlcIdx)+"GLItemTag");
			Vector<String[]> inputSerV = allDataHash.get(sdlcV.get(sdlcIdx)+"GLItem-Ser");
			Vector<String[]> inputPhaseV = allDataHash.get(sdlcV.get(sdlcIdx)+"GLItem-Phase");
			Vector<String[]> inputSysV = allDataHash.get("Sys-"+sdlcV.get(sdlcIdx)+"GLItem");
			Vector<String[]> inputDataV = allDataHash.get("Data-"+sdlcV.get(sdlcIdx)+"GLItem");

			for (int i=0;i<retList.size();i++)
			{
				String[] retLine = retList.get(i);
				String dataStr = retLine[1];
				String serStr = retLine[0];
				String sysStr = retLine[2];
				String[] inputLine = new String[2];
				//create rows for glitem-tag sheets first
				inputLine[0] = dataStr + "%" + serStr + "%" + sysStr + "%"+"Consumer"+"%"+sdlcV.get(sdlcIdx);
				inputLine[1] = "Consumer";
				inputTagV.addElement(inputLine);

				//create rows for glitem-ser, only have to chnage the 2nd column
				inputLine = new String[2];
				inputLine[0] = dataStr + "%" + serStr + "%" + sysStr + "%"+"Consumer"+"%"+sdlcV.get(sdlcIdx);
				inputLine[1] = serStr;
				inputSerV.addElement(inputLine);

				//create rows for glitem-phase, only have to change 2nd column
				inputLine = new String[2];
				inputLine[0] = dataStr + "%" + serStr + "%" + sysStr + "%"+"Consumer"+"%"+sdlcV.get(sdlcIdx);
				inputLine[1] = (String) sdlcV.get(sdlcIdx);
				inputPhaseV.addElement(inputLine);

				//create rows for sys-glitem, only have to change 2nd column
				inputLine = new String[2];
				inputLine[0] = sysStr;
				inputLine[1] = dataStr + "%" + serStr + "%" + sysStr + "%"+"Consumer"+"%"+sdlcV.get(sdlcIdx);
				inputSysV.addElement(inputLine);

				//create rows for data-glitem, only have to change 2nd column
				inputLine = new String[2];
				inputLine[0] = dataStr;
				inputLine[1] = dataStr + "%" + serStr + "%" + sysStr + "%"+"Consumer"+"%"+sdlcV.get(sdlcIdx);
				inputDataV.addElement(inputLine);
			}
			allDataHash.put(sdlcV.get(sdlcIdx)+"GLItemTag", inputTagV);
			allDataHash.put(sdlcV.get(sdlcIdx)+"GLItem-Ser", inputSerV);
			allDataHash.put(sdlcV.get(sdlcIdx)+"GLItem-Phase", inputPhaseV);
			allDataHash.put("Sys-"+sdlcV.get(sdlcIdx)+"GLItem", inputSysV);
			allDataHash.put("Data-"+sdlcV.get(sdlcIdx)+"GLItem", inputDataV);
		}
	}

	/**
	 * Generates a count of generic protocols for providers.
	 */
	public void genGenericProtocolCount()
	{
		String query = "SELECT DISTINCT (SAMPLE(?ser) AS ?Service) (SAMPLE(?data) AS ?IO) ?serIOprot (SAMPLE(?sys) AS ?Sys) (SAMPLE(?dProt) AS ?DProt) (COUNT(?dProt) AS ?DFormCount) WHERE { BIND(\"Generic\" AS ?sys) {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} BIND(<http://semoss.org/ontologies/Relation/Exposes> AS ?exposes) {?ser ?exposes ?data ;} BIND(<http://semoss.org/ontologies/Relation/Payload> AS ?payload) {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?icd ?payload ?data ;}  OPTIONAL { BIND(<http://semoss.org/ontologies/Relation/Has> AS ?has) {?dprot <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DProt> ;} {?icd ?has ?dprot ;} } BIND(COALESCE(?dprot, (URI(\"http://health.mil/ontologies/Concept/DProt/HTTPS-SOAP\"))) AS ?dProt) BIND(URI(CONCAT(\"http://health.mil/ontologies/Concept/\", SUBSTR(STR(?ser), 46), \"+\", SUBSTR(STR(?data), 49),\"+\", SUBSTR(STR(?dProt), 44))) AS ?serIOprot).} GROUP BY ?serIOprot";
		ArrayList<String[]> list  = retListFromQuery(query);
		for (int sdlcIdx=0 ; sdlcIdx<sdlcV.size();sdlcIdx++)
		{
			for (int i=0;i<list .size();i++)
			{
				String[] retLine = list.get(i);
				String dataStr = retLine[1];
				String serStr = retLine[0];
				String sysStr = "Generic";
				String protStr = retLine[5];
				String[] inputLine = new String[2];
				//create rows for glitem-tag sheets first
				String key = dataStr + "%" + serStr + "%" + sysStr + "%"+"Provider"+"%"+sdlcV.get(sdlcIdx);
				inputLine[0] = retLine[4];
				inputLine[1] = protStr;
				Vector<String[]> inputV = new Vector<String[]>();
				if (genericDProtHash.containsKey(key))
				{
					inputV = genericDProtHash.get(key);
				}
				else
				{
					inputV = new Vector<String[]>();
				}
				inputV.add(inputLine);
				genericDProtHash.put(key,  inputV);
			}
		}

		query = "SELECT DISTINCT (SAMPLE(?ser) AS ?Service) (SAMPLE(?data) AS ?IO) ?serIOform (SAMPLE(?sys) AS ?Sys) (SAMPLE(?dForm) AS ?DForm) (COUNT(?dForm) AS ?DFormCount) WHERE { BIND(\"Generic\" AS ?sys) {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} BIND(<http://semoss.org/ontologies/Relation/Exposes> AS ?exposes) {?ser ?exposes ?data ;} BIND(<http://semoss.org/ontologies/Relation/Payload> AS ?payload) {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?icd ?payload ?data ;} OPTIONAL { BIND(<http://semoss.org/ontologies/Relation/Has> AS ?has) {?dform <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DForm> ;} {?icd ?has ?dform ;} } BIND(COALESCE(?dform, (URI(\"http://health.mil/ontologies/Concept/DProt/XML\"))) AS ?dForm) BIND(URI(CONCAT(\"http://health.mil/ontologies/Concept/\", SUBSTR(STR(?ser), 46), \"+\", SUBSTR(STR(?data), 49),\"+\", SUBSTR(STR(?dForm), 44))) AS ?serIOform).  } GROUP BY ?serIOform";
		list  = retListFromQuery (query);
		for (int sdlcIdx=0 ; sdlcIdx<sdlcV.size();sdlcIdx++)
		{
			for (int i=0;i<list .size();i++)
			{
				String[] retLine = list.get(i);
				String dataStr = retLine[1];
				String serStr = retLine[0];
				String sysStr = "Generic";
				String protStr = retLine[5];
				String[] inputLine = new String[2];
				//create rows for glitem-tag sheets first
				String key = dataStr + "%" + serStr + "%" + sysStr + "%"+"Provider"+"%"+sdlcV.get(sdlcIdx);
				inputLine[0] = retLine[4];
				inputLine[1] = protStr;
				Vector<String[]> inputV = new Vector<String[]>();
				if (genericDFormHash.containsKey(key))
				{
					inputV = genericDFormHash.get(key);
				}
				else
				{
					inputV = new Vector<String[]>();
				}
				inputV.add(inputLine);
				genericDFormHash.put(key,  inputV);

			}
		}
	}

	/**
	 * Generates specific protocols based on data, service, system for consumers.
	 */
	public void genSpecificProtocol()
	{
		String query = this.genSpecificDProtQuery;
		ArrayList <String[]> list  = retListFromQuery (query);
		for (int sdlcIdx=0 ; sdlcIdx<sdlcV.size();sdlcIdx++)
		{
			for (int i=0;i<list .size();i++)
			{
				String[] retLine = list.get(i);
				String dataStr = retLine[1];
				String serStr = retLine[0];
				String sysStr = retLine[2];
				String protStr = retLine[3];
				//create rows for glitem-tag sheets first
				String key = dataStr + "%" + serStr + "%" + sysStr + "%"+"Consumer"+"%"+sdlcV.get(sdlcIdx);
				Vector<String> inputV = new Vector<String>();
				if (specificDProtHash.containsKey(key))
				{
					inputV = specificDProtHash.get(key);
				}
				else
				{
					inputV = new Vector<String>();
				}
				inputV.add(protStr);
				specificDProtHash.put(key,  inputV);
				//create rows for glitem-tag sheets first
			}
		}

		query = this.genSpecificDFormQuery;
		list  = retListFromQuery (query);
		for (int sdlcIdx=0; sdlcIdx < sdlcV.size(); sdlcIdx++)
		{
			for (int i=0; i < list.size(); i++)
			{
				String[] retLine = list.get(i);
				String dataStr = retLine[1];
				String serStr = retLine[0];
				String sysStr = retLine[2];
				String protStr = retLine[3];
				//create rows for glitem-tag sheets first
				String key = dataStr + "%" + serStr + "%" + sysStr + "%"+"Consumer"+"%"+sdlcV.get(sdlcIdx);
				Vector<String> inputV = new Vector<String>();
				if (specificDFormHash.containsKey(key))
				{
					inputV = specificDFormHash.get(key);
				}
				else
				{
					inputV = new Vector<String>();
				}
				inputV.add(protStr);
				specificDFormHash.put(key,  inputV);
				//create rows for glitem-tag sheets first

			}
		}
	}

	/**
	 * Creates the vector containing all the phases of the SDLC (system development life cycle).
	 * These include requirements, design, develop, test, and deploy.
	 */
	public void genSDLCVector()
	{
		sdlcV.addElement("Requirements");
		sdlcV.addElement("Design");
		sdlcV.addElement("Develop");
		sdlcV.addElement("Test");
		sdlcV.addElement("Deploy");
	}

	/**
	 * Runs all queries to obtain generic data list, generic BLU list, provider data list, provider BLU list, consumer data list,
	 * core tasks, sub core tasks, and system complexity.
	 */
	public void runAllQuery()
	{
		//get all data necessary for ESB instrumentation generic data pieces
		String query = "SELECT DISTINCT ?ser ?data ?sys WHERE { BIND(\"Generic\" AS ?sys) {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?exposes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Exposes>;} {?ser ?exposes ?data ;} }";
		genericDataList = retListFromQuery(query);

		//get all data necessary for ESB instrumentation generic blu pieces
		query = "SELECT DISTINCT ?ser ?blu ?sys WHERE { BIND(\"Generic\" AS ?sys) {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} {?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;} {?exposes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Exposes>;} {?ser ?exposes ?blu ;}} ";
		genericBLUList = retListFromQuery(query);
		
		fillProviderDataList();
		fillConsumerDataList();


		//get all data necessary for provider BLU pieces
		query = "SELECT DISTINCT ?ser ?blu ?sys WHERE { {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} {?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;} {?exposes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Exposes>;} {?ser ?exposes ?blu ;} {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?sys ?provide ?blu ;} }";
		providerBLUList = retListFromQuery(query);


		//get all coreTasks
		query = "SELECT DISTINCT ?BasisTarget ?TargetPhaseBasisCoreTask ?BasisCoreTask WHERE { {?BasisTarget <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BasisTarget> ;}  {?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;} {?TargetPhaseBasisCoreTask <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TargetPhaseBasisCoreTask> ;} {?BasisTarget ?has ?TargetPhaseBasisCoreTask ;} {?exists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/ExistsAs> ;} {?BasisCoreTask <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BasisCoreTask> ;} {?BasisCoreTask ?exists ?TargetPhaseBasisCoreTask ;} }";
		coreTaskList = retListFromFinancialQuery(query);

		//get all subCoreTasks
		query = "SELECT DISTINCT ?BasisTarget ?TargetPhaseBasisCoreTask ?TargetPhaseBasisSubTask WHERE { {?BasisTarget <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BasisTarget> ;}  {?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;} {?TargetPhaseBasisCoreTask <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TargetPhaseBasisCoreTask> ;} {?BasisTarget ?has ?TargetPhaseBasisCoreTask ;} {?includes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Includes> ;} {?BasisCoreTask <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BasisCoreTask> ;} {?TargetPhaseBasisCoreTask ?includes ?TargetPhaseBasisSubTask ;} }";
		subTaskList = retListFromFinancialQuery(query);

		fillSystemComplexityHash();
	}

	public void fillProviderDataList(){

		//get all data necessary for provider data pieces
		
		providerDataList = retListFromQuery(this.providerDataQuery1);

		//additional data necessary for provider data pieces due to redefinition of what it means for a system to be a creater of a piece of data
		if(!this.providerDataQuery2.isEmpty())
		{
			providerDataList.addAll(retListForAdditionalInfoQuery(this.providerDataQuery2, providerDataList));
		}
	}
	
	public void fillConsumerDataList(){

		//get all data necessary for consumer data pieces
		consumerList = retListFromQuery(this.consumerDataQuery);
	}
	
	public void fillSystemComplexityHash(){
		//get all systemComplexity
		String query = "SELECT DISTINCT ?sys ?complex WHERE { {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}  {?rated <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Rated>;} {?complex <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Complexity> ;} {?sys ?rated ?complex ;}} ";
		ArrayList<String[]> sysCompList = retListFromQuery(query);
		for (int i=0; i < sysCompList.size(); i++) {
			sysCompHash.put(sysCompList.get(i)[0], sysCompList.get(i)[1]);
		}
	}

	/**
	 * Prepares the hashtable containing all the data.
	 * First, puts in static tabs that are not dependent on lifecycle phase.
	 * Next, go through those that are dependent on the SDLC.
	 */
	public void prepareAllDataHash() {
		//first put in static tabs that are not dependent on phase
		String[] staticTabNames = {"GLItemCT-GLItemST", "GLItemCT-BasisCT","GLItemST-STBasisCompComp","GLItemSubTaskProp"};
		String[] staticHeader1Names = {"GLItemCoreTask", "GLItemCoreTask", "GLItemSubTask", "GLItemSubTask"};
		String[] staticHeader2Names = {"GLItemSubTask", "TargetPhaseBasisCoreTask", "TargetPhaseBasisSubTaskComplexityComplexity","Factor"};
		String[] staticRelation1Names = {"Relation", "Relation", "Relation", "Node"};
		String[] staticRelation2Names = {"Includes", "TypeOf", "Estimated","Ignore"};
		for(int i = 0; i<staticTabNames.length; i++){
			String tabName = staticTabNames[i];
			String header1 = staticHeader1Names[i];
			String header2 = staticHeader2Names[i];
			String relation1 = staticRelation1Names[i];
			String relation2 = staticRelation2Names[i];
			String[] headerRow = {header1, header2};
			String[] relationRow = {relation1, relation2};
			Vector<String[]> sheetV = new Vector<String[]>();
			sheetV.add(relationRow);
			sheetV.add(headerRow);
			allDataHash.put(tabName, sheetV);
		}

		//go through all SDLC
		String[] phaseTabNames = {"@Phase@GLItemTag", "@Phase@GLItem-Ser","@Phase@GLItem-Phase","Data-@Phase@GLItem","BLU-@Phase@GLItem",
				"Sys-@Phase@GLItem", "@Phase@GLItem-GLItemCT"};
		String[] phaseHeader1Names = {"@Phase@GLItem", "@Phase@GLItem", "@Phase@GLItem", "DataObject", "BusinessLogicUnit", 
				"System", "@Phase@GLItem"};
		String[] phaseHeader2Names = {"GLTag", "Service", "SDLCPhase","@Phase@GLItem","@Phase@GLItem",
				"@Phase@GLItem", "GLItemCoreTask"};
		String[] phaseRelation1Names = {"Relation", "Relation", "Relation", "Relation", "Relation", 
				"Relation", "Relation"};
		String[] phaseRelation2Names = {"TaggedBy", "Output", "BelongsTo","Input","Input",
				"Influences", "Includes"};
		for (int sdlcIdx=0 ; sdlcIdx<sdlcV.size();sdlcIdx++)
		{
			Hashtable<String, String> paramHash = new Hashtable<String, String>();
			paramHash.put("Phase", sdlcV.get(sdlcIdx));
			for(int i = 0; i < phaseTabNames.length; i++){
				String tabName = phaseTabNames[i];
				String header1 = phaseHeader1Names[i];
				String header2 = phaseHeader2Names[i];

				String filledTabName = Utility.fillParam(tabName, paramHash);
				String filledHeader1 = Utility.fillParam(header1, paramHash);
				String filledHeader2 = Utility.fillParam(header2, paramHash);

				String relation1 = phaseRelation1Names[i];
				String relation2 = phaseRelation2Names[i];

				String[] headerRow = {filledHeader1, filledHeader2};
				String[] relationRow = {relation1, relation2};
				Vector<String[]> sheetV = new Vector<String[]>();
				sheetV.add(relationRow);
				sheetV.add(headerRow);
				allDataHash.put(filledTabName, sheetV);
			}
		}
	}

	/**
	 * Exports the core relationships for services and systems and creates queries.
	 * These include System-Provide-Data, System-Provide-BLU, Service-Exposes-Data, and Service-Exposes-BLU.
	 */
	public void exportCoreRelationships() {
		ArrayList<String[]> nodeTypes = new ArrayList<String[]>();
		nodeTypes.add(new String[]{"System", "Provide", "DataObject"});
		nodeTypes.add(new String[]{"System", "Provide", "BusinessLogicUnit"});
		nodeTypes.add(new String[]{"Service", "Exposes", "DataObject"});
		nodeTypes.add(new String[]{"Service", "Exposes", "BusinessLogicUnit"});

		for(String[] nodes : nodeTypes) {
			String query = "SELECT ?in ?relationship ?out ?contains ?prop WHERE { "+ 
					"{?in <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/";
			query += nodes[0];
			query += "> ;}";

			query += " {?out <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/";
			query += nodes[2];
			query += "> ;}";

			query += "{?relationship <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/";
			query += nodes[1];
			query += "> ;} {?in ?relationship ?out ;} ";
			query += "OPTIONAL { {?contains <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> ;} {?relationship ?contains ?prop ;} } }";

			System.out.println("Processing query for core sheets " + query);
			Object[] returned = retListFromRelationshipQuery(query, nodes[1]);
			ArrayList<Object[]> list = (ArrayList<Object[]>) returned[0];
			HashSet<String> properties = (HashSet<String>) returned[1];

			String[] relation = {"Relation", nodes[1]};
			list.add(0, relation);
			String[] header = new String[properties.size()+2];
			Iterator<String> it = properties.iterator();
			header[0] = nodes[0];
			header[1] = nodes[2];
			for(int i = 0; i < properties.size(); i++) {
				header[i+2] = it.next();
			}
			list.add(1, header);
			Vector<String[]> results = new Vector<String[]>();
			for(Object[] o : list) {
				String[] toAdd = new String[o.length];
				for(int i = 0; i < o.length; i++) {
					toAdd[i] = o[i].toString();
				}
				results.add(toAdd);
			}

			RelationshipLoadingSheetWriter relWriter = new RelationshipLoadingSheetWriter();
			Hashtable<String, Vector<String[]>> temp = new Hashtable<String, Vector<String[]>>();
			temp.put("tempVector", results);
			temp = relWriter.prepareLoadingSheetExport(temp);
			nodes[0] = nodes[0].replace("System", "Sys").replace("Service", "Ser");
			nodes[2] = nodes[2].replace("DataObject", "Data").replace("BusinessLogicUnit", "BLU");
			allDataHash.put(nodes[0]+"-"+nodes[2], temp.get("tempVector"));
		}

		exportHWSWInfo();
	}

	/**
	 * Exports information about hardware and software for a given system.
	 */
	public void exportHWSWInfo() {
		String query = "";

		query += "SELECT ?sys ?rel3 ?hwv2 ?contains ?prop WHERE {" + 
				"{?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}" + 
				"{?hwm <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HardwareModule> ;}" + 
				"{?rel1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has> ;}" + 
				"{?sys ?rel1 ?hwm ;}" + 
				"{?hwv1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HardwareVersion> ;}" + 
				"{?rel2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TypeOf> ;}" + 
				"{?hwm ?rel2 ?hwv1 ;}" + 
				"{?hwv2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HardwareVersion> ;}" + 
				"{?rel3 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Upgrade> ;}" + 
				"{?hwv1 ?rel3 ?hwv2 ;}" +   
				"{?contains <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Contains/Price> ;} {?hwv2 ?contains ?prop ;} }";
		System.out.println("Processing hardware query  " + query);
		Object[] returned = retListFromRelationshipQuery(query, "Influences");
		ArrayList<Object[]> list = (ArrayList<Object[]>) returned[0];
		HashSet<String> properties = (HashSet<String>) returned[1];

		String[] relation = {"Relation", "Influences"};
		list.add(0, relation);
		String[] header = new String[properties.size()+2];
		Iterator<String> it = properties.iterator();
		header[0] = "System";
		header[1] = "SystemHWUpgradeGLItem";
		for(int i = 0; i < properties.size(); i++) {
			header[i+2] = it.next();
		}
		list.add(1, header);
		Vector<String[]> results = new Vector<String[]>();
		for(Object[] o : list) {
			String[] toAdd = new String[o.length];
			for(int i = 0; i < o.length; i++) {
				if(i == 2 && !properties.contains(o[i].toString())) {
					toAdd[i] = o[0].toString() + "%" + o[i].toString() + "%UpgradeGLItem";
				} else {
					toAdd[i] = o[i].toString();
				}
			}
			results.add(toAdd);
		}

		RelationshipLoadingSheetWriter relWriter = new RelationshipLoadingSheetWriter();
		Hashtable<String, Vector<String[]>> temp = new Hashtable<String, Vector<String[]>>();
		temp.put("tempVector", results);
		temp = relWriter.prepareLoadingSheetExport(temp);
		allDataHash.put("Sys"+"-"+"SysHWUpgradeGLItem", temp.get("tempVector"));
	}

	/**
	 * Exports site relationships for <System><DeployedAt><SystemDCSite> relation.
	 * Add relevant data to each system DC Site.
	 */
	public void exportSiteRelationships() {
		String subject = "System";
		String pred = "DeployedAt";
		String object = "SystemDCSite";

		String query = "SELECT ?in ?relationship ?out WHERE {"
				+ "{?in <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/" + subject + "> ; }"
				+ "{?out <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/" + object + "> ;} "
				+ "{?relationship <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/" + pred + "> ;} "
				+ "{?in ?relationship ?out ;} }";

		ArrayList<String[]> list = retListFromQuery(query);

		HashMap<String, String> siteToSystem = new HashMap<String, String>();
		for(String[] vals : list) {
			siteToSystem.put(vals[2], vals[0]);
		}

		//Initialize one vector for each relationship being exported
		Vector<String[]> GLItem_GLTag = new Vector<String[]>();
		Vector<String[]> sys_GLItem = new Vector<String[]>();
		Vector<String[]> GLItem_SDLCPhase = new Vector<String[]>();
		Vector<String[]> sysDCSite_GLItem = new Vector<String[]>();
		Vector<String[]> sys_sysDCSite = new Vector<String[]>();
		for(String[] s : list) {
			s[1] = s[2];
			sys_sysDCSite.add(s);
		}

		//For each SystemDCSite, add relevant data including creating GLItems
		for(String key : siteToSystem.keySet()) {
			String GLItem = key + "%SiteDeployment";
			GLItem_GLTag.add(new String[] {GLItem, "Consumer"});
			sys_GLItem.add(new String[] {siteToSystem.get(key), GLItem});
			GLItem_SDLCPhase.add(new String[] {GLItem, "Deploy"});
			sysDCSite_GLItem.add(new String[] {key, GLItem});
		}

		Comparator<String[]> comp = new Comparator<String[]>() {
			@Override
			public int compare(String[] arr1, String[] arr2) {
				return arr1[0].compareTo(arr2[0]);
			}
		};
		Collections.sort(GLItem_GLTag, comp);
		Collections.sort(sys_GLItem, comp);
		Collections.sort(GLItem_SDLCPhase, comp);
		Collections.sort(sysDCSite_GLItem, comp);
		Collections.sort(sys_sysDCSite, comp);

		//Adding header rows to each relationship data vector
		GLItem_GLTag.add(0, new String[] {"Relation", "TaggedBy"});
		GLItem_GLTag.add(1, new String[] {"SystemDCSiteGLItem", "GLTag"});
		sys_GLItem.add(0, new String[] {"Relation", "Influences"});
		sys_GLItem.add(1, new String[] {"System", "SystemDCSiteGLItem"});
		GLItem_SDLCPhase.add(0, new String[] {"Relation", "BelongsTo"});
		GLItem_SDLCPhase.add(1, new String[] {"SystemDCSiteGLItem", "SDLCPhase"});
		sysDCSite_GLItem.add(0, new String[] {"Relation", "Input"});
		sysDCSite_GLItem.add(1, new String[] {"SystemDCSite", "SystemDCSiteGLItem"});
		sys_sysDCSite.add(0, new String[] {"Relation", "DeployedAt"});
		sys_sysDCSite.add(1, new String[] {"System", "SystemDCSite"});

		allDataHash.put("SystemDCSiteGLItem-GLTag", GLItem_GLTag);
		allDataHash.put("System-SystemDCSiteGLItem", sys_GLItem);
		allDataHash.put("SystemDCSiteGLItem-SDLCPhase", GLItem_SDLCPhase);
		allDataHash.put("SystemDCSite-SystemDCSiteGLItem", sysDCSite_GLItem);
		allDataHash.put("System-SystemDCSite", sys_sysDCSite);
	}

	/**
	 * Returns the list of data from running a query on an engine.
	 * @param query String
	 * @return ArrayList 	Returned data from query. 
	 */
	public ArrayList<String[]> retListFromQuery(String query)
	{
		ArrayList<String[]> list = new ArrayList<String[]>();
		if (coreEngine == null){
			JComboBox<String> changedDBComboBox = (JComboBox<String>) DIHelper.getInstance().getLocalProp(Constants.CHANGED_DB_COMBOBOX);
			String changedDB = (String) changedDBComboBox.getSelectedItem();
			coreEngine = (IEngine)DIHelper.getInstance().getLocalProp(changedDB);
		}
		
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(coreEngine, query);

		/*SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(coreEngine);
		wrapper.executeQuery();
		// get the bindings from it
		*/
		
		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				ISelectStatement sjss = wrapper.next();

				String [] values = new String[names.length];
				boolean filledData = true;
				for(int colIndex = 0;colIndex < names.length;colIndex++)
				{
					if(sjss.getVar(names[colIndex]) != null)
					{
						values[colIndex] = sjss.getVar(names[colIndex])+"";
					}
					else {
						filledData = false;
						break;
					}
				}
				if(filledData) {
					list.add(values);
				}
			}
		} 
		catch (RuntimeException e) {
			e.printStackTrace();
		}
		return list;
	}

	/**
	 * Returns the list of data from running a query on an engine.
	 * @param query String
	 * @param originalList 
	 * @return ArrayList 	Returned data from query. 
	 */
	public ArrayList<String[]> retListForAdditionalInfoQuery(String query, ArrayList<String[]> originalList)
	{
		ArrayList<String[]> newListItems = new ArrayList<String[]>();
		if (coreEngine == null){
			JComboBox<String> changedDBComboBox = (JComboBox<String>) DIHelper.getInstance().getLocalProp(Constants.CHANGED_DB_COMBOBOX);
			String changedDB = (String) changedDBComboBox.getSelectedItem();
			coreEngine = (IEngine)DIHelper.getInstance().getLocalProp(changedDB);
		}
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(coreEngine, query);

		/*SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(coreEngine);
		wrapper.executeQuery();*/
		// get the bindings from it

		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				ISelectStatement sjss = wrapper.next();
				String [] values = new String[names.length];
				boolean filledData = true;
				for(int colIndex = 0;colIndex < names.length; colIndex++)
				{
					if(sjss.getVar(names[colIndex]) != null)
					{
						values[colIndex] = sjss.getVar(names[colIndex])+"";
					}
					else {
						filledData = false;
						break;
					}
				}
				if(filledData && !originalList.contains(values)) 
				{
					newListItems.add(values);
				}
			}
		} 
		catch (RuntimeException e) {
			e.printStackTrace();
		}
		return newListItems;
	}

	/**
	 * After running a query on the TAP Cost database, returns the results.
	 * @param query String
	 * @return ArrayList	Results from financial query. 
	 */
	public ArrayList<String[]> retListFromFinancialQuery(String query)
	{
		ArrayList<String[]> list = new ArrayList<String[]>();
		JList repoList = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		// get the selected repository
		Object[] repo = (Object[])repoList.getSelectedValues();
		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp("TAP_Cost_Data");
		
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);

		/*
		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();
		// get the bindings from it
*/
		
		int count = 0;
		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				ISelectStatement sjss = wrapper.next();

				String [] values = new String[names.length];
				boolean filledData = true;
				for(int colIndex = 0;colIndex < names.length;colIndex++)
				{
					if(sjss.getVar(names[colIndex]) != null)
					{
						values[colIndex] = sjss.getVar(names[colIndex])+"";
					}
					else {
						filledData = false;
						break;
					}
				}
				if(filledData)
					list.add(count, values);
				count++;
			}
		} 
		catch (RuntimeException e) {
		}
		return list;
	}

	/**
	 * Runs a query on an engine and returns results relating only to a specified relationship.
	 * @param query String
	 * @param relationship String
	 * @return Object[]	Results from the query. 
	 */
	public Object[] retListFromRelationshipQuery (String query, String relationship) {
		ArrayList<Object[]> list = new ArrayList<Object[]>();
		if (coreEngine == null){
			JComboBox<String> changedDBComboBox = (JComboBox<String>) DIHelper.getInstance().getLocalProp(Constants.CHANGED_DB_COMBOBOX);
			String changedDB = (String) changedDBComboBox.getSelectedItem();
			coreEngine = (IEngine)DIHelper.getInstance().getLocalProp(changedDB);
		}

		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(coreEngine, query);

		/*SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(coreEngine);
		wrapper.executeQuery();
		*/
		
		int count = 0;
		String[] names = wrapper.getVariables();
		HashSet<String> properties = new HashSet<String>();
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext()) {
				ISelectStatement sjss = wrapper.next();
				Object [] values = new Object[names.length];
				boolean filledData = true;

				for(int colIndex = 0;colIndex < names.length;colIndex++) {
					if(sjss.getVar(names[colIndex]) != null && !sjss.getVar(names[colIndex]).toString().equals(relationship)) {
						if(colIndex == 3 && !sjss.getVar(names[colIndex]).toString().isEmpty()) {
							properties.add((String) sjss.getVar(names[colIndex]));
						}
						values[colIndex] = sjss.getVar(names[colIndex]);
					}
					else {
						filledData = false;
						break;
					}
				}
				if(filledData) {
					list.add(count, values);
					count++;
				}
			}
		} 
		catch (RuntimeException e) {
			e.printStackTrace();
		}

		Collections.sort(list, new Comparator<Object[]>() {
			public int compare(Object[] a, Object[] b) {
				int comp = a[0].toString().compareTo(b[0].toString());
				if(comp == 0) {
					return a[2].toString().compareTo(b[2].toString());
				}
				return comp;
			}
		});

		return new Object[] {list, properties};
	}
	
	public void setGenSpecificDProtQuery(String q){
		this.genSpecificDProtQuery = q;
	}
	
	public void setGenSpecificDFormQuery(String q){
		this.genSpecificDFormQuery = q;
	}
	
	public void setCoreEngine(IEngine coreEngine){
		this.coreEngine = coreEngine;
	}

	public void setProviderDataQuery1(String providerDataQuery1) {
		this.providerDataQuery1 = providerDataQuery1;
	}

	public void setProviderDataQuery2(String providerDataQuery2) {
		this.providerDataQuery2 = providerDataQuery2;
	}

	public void setConsumerDataQuery(String consumerDataQuery) {
		this.consumerDataQuery = consumerDataQuery;
	}
	
	public Hashtable<String, Vector<String[]>> getAllDataHash() {
		return allDataHash;
	}

}
