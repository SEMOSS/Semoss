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
package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;

import javax.swing.JList;

import prerna.poi.main.POIWriter;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * This class generates items for the general ledger. 
 */
public class GLItemGenerator {
	
	public Hashtable allDataHash = new Hashtable();
	public Hashtable genericDProtHash = new Hashtable();
	public Hashtable genericDFormHash = new Hashtable();
	public Hashtable specificDProtHash = new Hashtable();
	public Hashtable specificDFormHash = new Hashtable();
	public Vector sdlcV = new Vector();
	ArrayList <String[]> genericDataList = new ArrayList();
	ArrayList <String[]> genericBLUList	= new ArrayList();
	ArrayList <String[]> providerDataList = new ArrayList();
	ArrayList <String[]> providerBLUList = new ArrayList();
	ArrayList <String[]> consumerList = new ArrayList();
	ArrayList <String[]> coreTaskList = new ArrayList();
	ArrayList <String[]> subTaskList = new ArrayList();
	Hashtable sysCompHash = new Hashtable();
	
	/**
	 * Constructor for GLItemGenerator.
	 */
	public GLItemGenerator()
	{
		
	}
	
	/**
	 * Generates the list of GL items.
	 */
	public void genList()
	{
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
		POIWriter poiwrite = new POIWriter();
		poiwrite.runExport(allDataHash, Constants.GLITEM_CORE_LOADING_SHEET, null, true);
	}
	
	/**
	 * Generates factors for the all data hashtable (subtask properties and CT/ST).
	 */
	public void genFactors() {
		Vector subV = (Vector) allDataHash.get("GLItemCT-GLItemST");
		Vector newV = (Vector) allDataHash.get("GLItemSubTaskProp");
		//starting at 2 because tthe first array has bs taht Bill added
		for (int i = 2; i<subV.size();i++)
		{
			String[] rowArray= (String[]) subV.get(i);
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
		Vector glItemV;
		for (int sdlcIdx=0 ; sdlcIdx<sdlcV.size();sdlcIdx++)
		{
			glItemV = (Vector) allDataHash.get("Data-"+sdlcV.get(sdlcIdx)+"GLItem");
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
			glItemV = (Vector) allDataHash.get("BLU-"+sdlcV.get(sdlcIdx)+"GLItem");
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
	public void processDataGenericGL(String glString,  String phase)
	{
		Vector coreTaskV;
		Vector coreTaskBasisV;
		Vector subTaskV;
		Vector subTaskCompV;

		coreTaskV = (Vector) allDataHash.get(phase+"GLItem-GLItemCT");
		coreTaskBasisV = (Vector) allDataHash.get("GLItemCT-BasisCT");
		subTaskV = (Vector) allDataHash.get("GLItemCT-GLItemST");
		subTaskCompV = (Vector) allDataHash.get("GLItemST-STBasisCompComp");

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
		

		
		Vector dFormV = (Vector) genericDFormHash.get(glString);
		if (dFormV == null)
		{
			dFormV = new Vector();
		}
		for (int i=0;i<dFormV.size();i++)
		{
			inputArray = new String[2];
			String[] vecArray= (String[]) dFormV.get(i);
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
		
		Vector dProtV = (Vector) genericDProtHash.get(glString);
		if (dProtV == null)
		{
			dProtV = new Vector();
		}
		for (int i=0;i<dProtV.size();i++)
		{
			inputArray = new String[2];
			String[] vecArray= (String[]) dProtV.get(i);
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
		Vector coreTaskV;
		Vector coreTaskBasisV;
		Vector subTaskV;
		Vector subTaskCompV;

		coreTaskV = (Vector) allDataHash.get(phase+"GLItem-GLItemCT");
		coreTaskBasisV = (Vector) allDataHash.get("GLItemCT-BasisCT");
		subTaskV = (Vector) allDataHash.get("GLItemCT-GLItemST");
		subTaskCompV = (Vector) allDataHash.get("GLItemST-STBasisCompComp");
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
		String complexity = (String) sysCompHash.get(split[2]);
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
		Vector coreTaskV;
		Vector coreTaskBasisV;
		Vector subTaskV;
		Vector subTaskCompV;

		coreTaskV = (Vector) allDataHash.get(phase+"GLItem-GLItemCT");
		coreTaskBasisV = (Vector) allDataHash.get("GLItemCT-BasisCT");
		subTaskV = (Vector) allDataHash.get("GLItemCT-GLItemST");
		subTaskCompV = (Vector) allDataHash.get("GLItemST-STBasisCompComp");

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
		Vector coreTaskV;
		Vector coreTaskBasisV;
		Vector subTaskV;
		Vector subTaskCompV;

		coreTaskV = (Vector) allDataHash.get(phase+"GLItem-GLItemCT");
		coreTaskBasisV = (Vector) allDataHash.get("GLItemCT-BasisCT");
		subTaskV = (Vector) allDataHash.get("GLItemCT-GLItemST");
		subTaskCompV = (Vector) allDataHash.get("GLItemST-STBasisCompComp");
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
		String complexity = (String) sysCompHash.get(split[2]);
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
		Vector coreTaskV;
		Vector coreTaskBasisV;
		Vector subTaskV;
		Vector subTaskCompV;
		coreTaskV = (Vector) allDataHash.get(phase+"GLItem-GLItemCT");
		coreTaskBasisV = (Vector) allDataHash.get("GLItemCT-BasisCT");
		subTaskV = (Vector) allDataHash.get("GLItemCT-GLItemST");
		subTaskCompV = (Vector) allDataHash.get("GLItemST-STBasisCompComp");
		String[] inputArray = new String[2];
		
		//system complexity piece
		String[] split = glString.split("%");
		String complexity = (String) sysCompHash.get(split[2]);
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
		
		Vector dProtV = (Vector) specificDProtHash.get(glString);
		if (dProtV == null)
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
			inputArray[1] = glString+"%HTTPS/SOAP";
			subTaskV.addElement(inputArray);
			
			basisSubTask = getBasisCoreTask(inputArray[1]);		
			inputArray = new String[2];
			inputArray[0] = glString+"%HTTPS/SOAP";
			inputArray[1] = basisSubTask+"%Simple%"+complexity;
			subTaskCompV.addElement(inputArray);
		}
		else
		{
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
		}
		Vector dFormV = (Vector) specificDFormHash.get(glString);
		if (dFormV == null)
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
			inputArray[1] = glString+"%XML";
			subTaskV.addElement(inputArray);
			
			//system complexity comes second in this case
			
			basisSubTask = getBasisCoreTask(inputArray[1]);		
			inputArray = new String[2];
			inputArray[0] = glString+"%XML";
			inputArray[1] = basisSubTask+"%Simple%"+complexity;
			subTaskCompV.addElement(inputArray);
		}
		else
		{
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
		}
		allDataHash.put(phase+"GLItem-GLItemCT", coreTaskV);
		allDataHash.put("GLItemCT-BasisCT", coreTaskBasisV);
		allDataHash.put("GLItemCT-GLItemST", subTaskV);
		allDataHash.put("GLItemST-STBasisCompComp", subTaskCompV);
	}
	/**
	 * Splits a string to obtain the GL basis core task.
	 * @param glCoreTask String to be split.
	
	 * @return String	Basis core task. */
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
			Vector inputTagV;
			Vector inputSerV;
			Vector inputPhaseV;
			Vector inputDataV;
	
			inputTagV = (Vector) allDataHash.get(sdlcV.get(sdlcIdx)+"GLItemTag");
			inputSerV = (Vector) allDataHash.get(sdlcV.get(sdlcIdx)+"GLItem-Ser");
			inputPhaseV = (Vector) allDataHash.get(sdlcV.get(sdlcIdx)+"GLItem-Phase");
			inputDataV = (Vector) allDataHash.get("Data-"+sdlcV.get(sdlcIdx)+"GLItem");

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
			Vector inputTagV;
			Vector inputSerV;
			Vector inputPhaseV;
			Vector inputBLUV;

			inputTagV = (Vector) allDataHash.get(sdlcV.get(sdlcIdx)+"GLItemTag");
			inputSerV = (Vector) allDataHash.get(sdlcV.get(sdlcIdx)+"GLItem-Ser");
			inputPhaseV = (Vector) allDataHash.get(sdlcV.get(sdlcIdx)+"GLItem-Phase");
			inputBLUV = (Vector) allDataHash.get("BLU-"+sdlcV.get(sdlcIdx)+"GLItem");


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
			String key = sdlcV.get(sdlcIdx)+"GLItemTag";
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
			Vector inputTagV;
			Vector inputSerV;
			Vector inputPhaseV;
			Vector inputSysV;
			Vector inputDataV;

			inputTagV = (Vector) allDataHash.get(sdlcV.get(sdlcIdx)+"GLItemTag");
			inputSerV = (Vector) allDataHash.get(sdlcV.get(sdlcIdx)+"GLItem-Ser");
			inputPhaseV = (Vector) allDataHash.get(sdlcV.get(sdlcIdx)+"GLItem-Phase");
			inputSysV = (Vector) allDataHash.get("Sys-"+sdlcV.get(sdlcIdx)+"GLItem");
			inputDataV = (Vector) allDataHash.get("Data-"+sdlcV.get(sdlcIdx)+"GLItem");

			//not going through all SDLC because datafed deploy does not exist
			for (int i=0;i<retList.size();i++)
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
		for (int sdlcIdx=0 ; sdlcIdx<sdlcV.size();sdlcIdx++)
		{
			Vector inputTagV;
			Vector inputSerV;
			Vector inputPhaseV;
			Vector inputSysV;
			Vector inputBLUV;

			inputTagV = (Vector) allDataHash.get(sdlcV.get(sdlcIdx)+"GLItemTag");
			inputSerV = (Vector) allDataHash.get(sdlcV.get(sdlcIdx)+"GLItem-Ser");
			inputPhaseV = (Vector) allDataHash.get(sdlcV.get(sdlcIdx)+"GLItem-Phase");
			inputSysV = (Vector) allDataHash.get("Sys-"+sdlcV.get(sdlcIdx)+"GLItem");
			inputBLUV = (Vector) allDataHash.get("BLU-"+sdlcV.get(sdlcIdx)+"GLItem");

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
		for (int sdlcIdx=0 ; sdlcIdx<sdlcV.size();sdlcIdx++)
		{
			Vector inputTagV;
			Vector inputSerV;
			Vector inputPhaseV;
			Vector inputSysV;
			Vector inputDataV;

			inputTagV = (Vector) allDataHash.get(sdlcV.get(sdlcIdx)+"GLItemTag");
			inputSerV = (Vector) allDataHash.get(sdlcV.get(sdlcIdx)+"GLItem-Ser");
			inputPhaseV = (Vector) allDataHash.get(sdlcV.get(sdlcIdx)+"GLItem-Phase");
			inputSysV = (Vector) allDataHash.get("Sys-"+sdlcV.get(sdlcIdx)+"GLItem");
			inputDataV = (Vector) allDataHash.get("Data-"+sdlcV.get(sdlcIdx)+"GLItem");

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
		String tom = "check";
	}
	/**
	 * Generates a count of generic protocols for providers.
	 */
	public void genGenericProtocolCount()
	{
		String query = "SELECT DISTINCT (SAMPLE(?ser) AS ?Service) (SAMPLE(?data) AS ?IO) ?serIOprot (SAMPLE(?sys) AS ?Sys) (SAMPLE(?dProt) AS ?DProt) (COUNT(?dProt) AS ?DFormCount) WHERE { BIND(\"Generic\" AS ?sys) {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} BIND(<http://semoss.org/ontologies/Relation/Exposes> AS ?exposes) {?ser ?exposes ?data ;} BIND(<http://semoss.org/ontologies/Relation/Payload> AS ?payload) {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?icd ?payload ?data ;}  OPTIONAL { BIND(<http://semoss.org/ontologies/Relation/Has> AS ?has) {?dprot <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DProt> ;} {?icd ?has ?dprot ;} } BIND(COALESCE(?dprot, (URI(\"http://semoss.org/ontologies/Concept/DProt/HTTPS-SOAP\"))) AS ?dProt) BIND(URI(CONCAT(\"http://semoss.org/ontologies/Concept/\", SUBSTR(STR(?ser), 51), \"+\", SUBSTR(STR(?data), 54),\"+\", SUBSTR(STR(?dProt), 49))) AS ?serIOprot).} GROUP BY ?serIOprot";
		ArrayList <String[]> list  = retListFromQuery (query);
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
				Vector inputV = new Vector();
				if (genericDProtHash.containsKey(key))
				{
					inputV = (Vector) genericDProtHash.get(key);
				}
				else
				{
					inputV = new Vector();
				}
				inputV.add(inputLine);
				
				genericDProtHash.put(key,  inputV);
	
			}
		}
		
		query = "SELECT DISTINCT (SAMPLE(?ser) AS ?Service) (SAMPLE(?data) AS ?IO) ?serIOform (SAMPLE(?sys) AS ?Sys) (SAMPLE(?dForm) AS ?DForm) (COUNT(?dForm) AS ?DFormCount) WHERE { BIND(\"Generic\" AS ?sys) {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} BIND(<http://semoss.org/ontologies/Relation/Exposes> AS ?exposes) {?ser ?exposes ?data ;} BIND(<http://semoss.org/ontologies/Relation/Payload> AS ?payload) {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?icd ?payload ?data ;} OPTIONAL { BIND(<http://semoss.org/ontologies/Relation/Has> AS ?has) {?dform <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DForm> ;} {?icd ?has ?dform ;} } BIND(COALESCE(?dform, (URI(\"http://semoss.org/ontologies/Concept/DProt/XML\"))) AS ?dForm) BIND(URI(CONCAT(\"http://semoss.org/ontologies/Concept/\", SUBSTR(STR(?ser), 51), \"+\", SUBSTR(STR(?data), 54),\"+\", SUBSTR(STR(?dForm), 49))) AS ?serIOform).  } GROUP BY ?serIOform";
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
				Vector inputV = new Vector();
				if (genericDFormHash.containsKey(key))
				{
					inputV = (Vector) genericDFormHash.get(key);
				}
				else
				{
					inputV = new Vector();
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
		String query = "SELECT DISTINCT ?ser ?data ?sys (COALESCE(?dprot, (URI(\"http://semoss.org/ontologies/Concept/DProt/HTTPS-SOAP\"))) AS ?Prot) WHERE { {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;}  BIND(<http://semoss.org/ontologies/Relation/Exposes> AS ?exposes) {?ser ?exposes ?data ;} {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} BIND(<http://semoss.org/ontologies/Relation/Consume> AS ?downstream) {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?icd ?downstream ?sys ;} BIND(<http://semoss.org/ontologies/Relation/Payload> AS ?payload) {?icd ?payload ?data ;} OPTIONAL{ BIND(<http://semoss.org/ontologies/Relation/Has> AS ?has) {?dprot <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DProt> ;} {?icd ?has ?dprot ;}} }";
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
				String[] inputLine = new String[2];
				//create rows for glitem-tag sheets first
				String key = dataStr + "%" + serStr + "%" + sysStr + "%"+"Consumer"+"%"+sdlcV.get(sdlcIdx);
				Vector inputV = new Vector();
				if (specificDProtHash.containsKey(key))
				{
					inputV = (Vector) specificDProtHash.get(key);
				}
				else
				{
					inputV = new Vector();
				}
				inputV.add(protStr);
				
				specificDProtHash.put(key,  inputV);
				//create rows for glitem-tag sheets first
	
			}
		}
		
		query = "SELECT DISTINCT ?ser ?data ?sys (COALESCE(?dform, (URI(\"http://semoss.org/ontologies/Concept/DForm/XML\"))) AS ?Form) WHERE { {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;}  BIND(<http://semoss.org/ontologies/Relation/Exposes> AS ?exposes) {?ser ?exposes ?data ;} {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} BIND(<http://semoss.org/ontologies/Relation/Consume> AS ?downstream) {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?icd ?downstream ?sys ;} BIND(<http://semoss.org/ontologies/Relation/Payload> AS ?payload) {?icd ?payload ?data ;} OPTIONAL{ BIND(<http://semoss.org/ontologies/Relation/Has> AS ?has) {?dform <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DForm> ;} {?icd ?has ?dform ;}} }";
		list  = retListFromQuery (query);
		for (int sdlcIdx=0 ; sdlcIdx<sdlcV.size();sdlcIdx++)
		{
			for (int i=0;i<list .size();i++)
			{
				String[] retLine = list.get(i);
				String dataStr = retLine[1];
				String serStr = retLine[0];
				String sysStr = retLine[2];
				String protStr = retLine[3];
				String[] inputLine = new String[2];
				//create rows for glitem-tag sheets first
				String key = dataStr + "%" + serStr + "%" + sysStr + "%"+"Consumer"+"%"+sdlcV.get(sdlcIdx);
				Vector inputV = new Vector();
				if (specificDFormHash.containsKey(key))
				{
					inputV = (Vector) specificDFormHash.get(key);
				}
				else
				{
					inputV = new Vector();
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
		genericDataList = retListFromQuery (query);
		
		//get all data necessary for ESB instrumentation generic blu pieces
		query = "SELECT DISTINCT ?ser ?blu ?sys WHERE { BIND(\"Generic\" AS ?sys) {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} {?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;} {?exposes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Exposes>;} {?ser ?exposes ?blu ;}} ";
		genericBLUList = retListFromQuery (query);
		
		//get all data necessary for provider data pieces
		query = "SELECT DISTINCT ?ser ?data ?sys WHERE { {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?exposes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Exposes>;} {?ser ?exposes ?data ;} {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?sys ?provide ?data ;} {?provide <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm ;} } BINDINGS ?crm {(\"C\")(\"M\")}";
		providerDataList = retListFromQuery (query);
		
		//get all data necessary for provider BLU pieces
		query = "SELECT DISTINCT ?ser ?blu ?sys WHERE { {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} {?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;} {?exposes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Exposes>;} {?ser ?exposes ?blu ;} {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?sys ?provide ?blu ;} }";
		providerBLUList = retListFromQuery (query);
		
		//get all data necessary for consumer data pieces
		query = "SELECT DISTINCT ?ser ?data ?sys WHERE { {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} BIND(<http://semoss.org/ontologies/Relation/Exposes> AS ?exposes) {?ser ?exposes ?data ;} {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?sys ?provide ?data ;}{BIND(<http://semoss.org/ontologies/Relation/Consume> AS ?downstream) {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?icd ?downstream ?sys ;} {?payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?icd ?payload ?data ;}} UNION {{ ?provide <http://semoss.org/ontologies/Relation/Contains/CRM> \"R\" ;} }}";
		consumerList = retListFromQuery (query);
	
		//get all coreTasks
		query = "SELECT DISTINCT ?BasisTarget ?TargetPhaseBasisCoreTask ?BasisCoreTask WHERE { {?BasisTarget <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BasisTarget> ;}  {?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;} {?TargetPhaseBasisCoreTask <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TargetPhaseBasisCoreTask> ;} {?BasisTarget ?has ?TargetPhaseBasisCoreTask ;} {?exists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/ExistsAs> ;} {?BasisCoreTask <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BasisCoreTask> ;} {?BasisCoreTask ?exists ?TargetPhaseBasisCoreTask ;} }";
		coreTaskList = retListFromFinancialQuery (query);
		
		//get all subCoreTasks
		query = "SELECT DISTINCT ?BasisTarget ?TargetPhaseBasisCoreTask ?TargetPhaseBasisSubTask WHERE { {?BasisTarget <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BasisTarget> ;}  {?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;} {?TargetPhaseBasisCoreTask <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TargetPhaseBasisCoreTask> ;} {?BasisTarget ?has ?TargetPhaseBasisCoreTask ;} {?includes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Includes> ;} {?BasisCoreTask <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BasisCoreTask> ;} {?TargetPhaseBasisCoreTask ?includes ?TargetPhaseBasisSubTask ;} }";
		subTaskList = retListFromFinancialQuery (query);
		
		//get all systemComplexity
		query = "SELECT DISTINCT ?sys ?complex WHERE { {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}  {?rated <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Rated>;} {?complex <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Complexity> ;} {?sys ?rated ?complex ;}} ";
		ArrayList<String[]> sysCompList = retListFromFinancialQuery (query);
		for (int i=0; i<sysCompList.size();i++)
		{
			sysCompHash.put(sysCompList.get(i)[0], sysCompList.get(i)[1]);
		}
		
	}
	
	/**
	 * Prepares the hashtable containing all the data.
	 * First, puts in static tabs that are not dependent on lifecycle phase.
	 * Next, go through those that are dependent on the SDLC.
	 */
	public void prepareAllDataHash(){
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
               Hashtable paramHash = new Hashtable();
               paramHash.put("Phase", sdlcV.get(sdlcIdx));
               for(int i = 0; i<phaseTabNames.length; i++){
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
	 * Returns the list of data from running a query on an engine.
	 * @param query String
	
	 * @return ArrayList 	Returned data from query. */
	public ArrayList retListFromQuery (String query)
	{
		ArrayList <String []> list = new ArrayList();
		JList repoList = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		// get the selected repository
		Object[] repo = (Object[])repoList.getSelectedValues();
		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(repo[0]+"");
		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();
		// get the bindings from it

		int count = 0;
		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				SesameJenaSelectStatement sjss = wrapper.next();
				
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
	 * After running a query on the TAP Cost database, returns the results.
	 * @param query String
	
	 * @return ArrayList	Results from financial query. */
	public ArrayList retListFromFinancialQuery (String query)
	{
		ArrayList <String []> list = new ArrayList();
		JList repoList = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		// get the selected repository
		Object[] repo = (Object[])repoList.getSelectedValues();
		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp("TAP_Cost_Data");
		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();
		// get the bindings from it

		int count = 0;
		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				SesameJenaSelectStatement sjss = wrapper.next();
				
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
}
