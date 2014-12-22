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

import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Locale;

import javax.swing.JCheckBox;
import javax.swing.JList;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.GridFilterData;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * This class contains functions about services that are used in performing estimation calculations.
 */
public class EstimationCalculationFunctions {
	GridFilterData gfd = new GridFilterData();
	Hashtable yearIdxTable = new Hashtable();
	Hashtable systemPhaseEstimate = new Hashtable();
	Hashtable systemPhaseDate = new Hashtable();
	public PrintWriter out= null;
	boolean serviceBoolean;
	Double hourlyRate;
	ArrayList<String> runTypes = new ArrayList<String>();
	/**
	 * Sets the service boolean.
	 * @param ser boolean
	 */
	public void setServiceBoolean(boolean ser){
		serviceBoolean = ser;
	}
	/**
	 * Constructor for EstimationCalculationFunctions.
	 */
	public EstimationCalculationFunctions()
	{
		yearIdxTable.put(2013, 2);
		yearIdxTable.put(2014, 3);
		yearIdxTable.put(2015, 4);
		yearIdxTable.put(2016, 5);
		yearIdxTable.put(2017, 6);
		yearIdxTable.put(2018, 7);
	}
	
	/**
	 * Sets the hourly rate.
	 * @param rate Double
	 */
	public void setHourlyRate(Double rate){
		this.hourlyRate=rate;
	}
	
	/**
	 * Sets the types.
	 * @param types 	ArrayList of types.
	 */
	public void setTypes(ArrayList types){
		runTypes = types;
	}
	/**
	 * Given a date, retrieve the month and year and make adjustments to get the year index.
	 * @param date Date
	
	 * @return Integer	Year index. */
	public Integer retYearIdx (Date date)
	{
		int year= date.getYear();
		int month = date.getMonth();
		month++;
		year = 1900+year;
		int yearIdx = (Integer) yearIdxTable.get(year);
		if (month>=10)
		{
			yearIdx++;
		}
		return yearIdx;
	}
	
	/**
	 * Given a date, gets the year and month and makes necessary adjustments to return the fiscal year.
	 * @param date Date
	
	 * @return Integer 	Fiscal year. */
	public Integer retFiscalYear (Date date)
	{
		int year= date.getYear();
		int month = date.getMonth();
		month++;
		year = 1900+year;
		if (month>=10)
		{
			year++;
		}
		return year;
	}
	
	/**
	 * Creates a hashtable of LOE and start date for each system.
	 * Creates an arraylist of specific data for each phase in the life cycle.
	 * @param system 	System name, in string format.
	
	 * @return ArrayList 	Retrieved data for a system in each phase of the life cycle. */
	public ArrayList processPhaseData(String system)
	{
		
		int loeIdx = 4;
		int dateIdx = 5;

		//we want a return of hashtable of LOE, startdate
		Hashtable phaseKeyMapping = new Hashtable();
		phaseKeyMapping.put("Requirements",0);
		phaseKeyMapping.put("Design",1);
		phaseKeyMapping.put("Develop",2);
		phaseKeyMapping.put("Test",3);
		phaseKeyMapping.put("Deploy",4);
		
		//will use this returnList to return all the required objects
		ArrayList <Object []> returnList = new ArrayList <Object[]>();
		//initialize arrayList
		for (int i = 0;i<5;i++)
		{
			returnList.add(i, new Object[6]);		
		}
		
		//standard index of all the phase arrays
		int phaseIdx = 0;
		int highestLOESetIdx = 1;
		int startDateIdx = 2;
		int endDateIdx = 3;
		int totalLOEIdx = 4;
		int dataFedIdx = 5;
		for (int queryIdx = 0; queryIdx< runTypes.size();queryIdx++)
		{
			String query = getQuery((String) runTypes.get(queryIdx));
			ArrayList <Object []> list = getSpecData(system, query);
		//process all return from query
			for (int i = 0; i<list.size();i++)
			{
				//phase is the first column return
				String phase = (String) list.get(i)[0];
				String gltag = (String) list.get(i)[6];
				Double loeInstance = (Double) list.get(i)[loeIdx];
				//Add 10000 fixed cost for all deployment as specified by
				if (runTypes.get(queryIdx).equals(Constants.TRANSITION_SPECIFIC_SITE_CONSUMER) && hourlyRate!=null)
					loeInstance = loeInstance + 10000/hourlyRate;
				int phaseIndex = (Integer) phaseKeyMapping.get(phase);
				//query already organized by highest LOE, will enter into each phaseArray
				//phaseArrays get initialized here as well
				if (returnList.get(phaseIndex)[0] == null)
				{
					Object[] phaseReturnArray = new Object[6];
					phaseReturnArray[highestLOESetIdx] = list.get(i);
					phaseReturnArray[totalLOEIdx]= loeInstance;
					phaseReturnArray[phaseIdx]=phase;
					//calculate data fed total
					if (runTypes.get(queryIdx).equals(Constants.TRANSITION_DATA_FEDERATION))
					{
						phaseReturnArray[dataFedIdx]=loeInstance;
					}
					returnList.remove(phaseIndex);
					returnList.add(phaseIndex, phaseReturnArray);
					
				}
				else
				{
					Object[] phaseReturnArray = returnList.get(phaseIndex);
					phaseReturnArray[totalLOEIdx] =(Double)phaseReturnArray[totalLOEIdx]+loeInstance;
					//calculate data fed total
					if (runTypes.get(queryIdx).equals(Constants.TRANSITION_DATA_FEDERATION) && phaseReturnArray[dataFedIdx]!=null)
					{
						phaseReturnArray[dataFedIdx]=(Double)phaseReturnArray[dataFedIdx]+loeInstance;
					}
					else if (runTypes.get(queryIdx).equals(Constants.TRANSITION_DATA_FEDERATION))
					{
						phaseReturnArray[dataFedIdx]=loeInstance;
					}
					returnList.remove(phaseIndex);
					returnList.add(phaseIndex, phaseReturnArray);
				}
	
				//check for highest LOE, only first element of query return can be highest because it's ordered by highest LOE in query
				if (i==0)
				{
					Object[] highestLOESet = (Object[]) returnList.get(phaseIndex)[highestLOESetIdx];
					if (highestLOESet[loeIdx]!=null)
					{
						Double highestLOE = (Double) highestLOESet[loeIdx];
						if (highestLOE < loeInstance)
						{
							returnList.get(phaseIndex)[highestLOESetIdx]=list.get(i);
						}
					}
				}
			}
		}

		//process last dates
		Date startDate = null;
		Date endDate = null;
		Date prevEndDate = null;
		for (int i = 0; i<returnList.size();i++)
		{

			//get the first return set that was returned from query and put in earlier
			Object[] highestLOESet = (Object[]) returnList.get(i)[highestLOESetIdx];
			Object[] phaseReturnArray = returnList.get(i);
			if (highestLOESet != null)
			{
				startDate = getStartDate (highestLOESet, prevEndDate, loeIdx, dateIdx);
				endDate = getEndDate (highestLOESet, startDate, loeIdx,dateIdx);
				prevEndDate = endDate;
			}
			
			phaseReturnArray[startDateIdx] = startDate;
			phaseReturnArray[endDateIdx] = endDate;
			returnList.remove(i);
			returnList.add(i, phaseReturnArray);
		}

		return returnList;
	}
	
	/**
	 * Gets the query used to run LOE calculation.
	 * If it is a service, obtain the filter services if a checkbox is selected.
	 * @param runType 	String used to get the query.
	
	 * @return String 	Query. */
	public String getQuery(String runType){
		//set query depending on the types array
		boolean check = true;
	
		String query = DIHelper.getInstance().getProperty((String) runType);
		//check to see if selected database contains LOEcalc.  If it doesn't, algorithm will fail
		String loeCalcCheckquery = "SELECT ?s ?p ?o WHERE{ BIND(<http://semoss.org/ontologies/Relation/Contains/LOEcalc> AS ?p) {?s ?p ?o}}";
		//check to see if the property LOEcalc already exists in the database.
		check =Utility.runCheck(loeCalcCheckquery);
			
		ServiceSelectPanel servicePanel = (ServiceSelectPanel) DIHelper.getInstance().getLocalProp(Constants. TRANSITION_SERVICE_PANEL);
		Enumeration<String> enumKey = servicePanel.checkBoxHash.keys();
		String filterServices = "";
		if(serviceBoolean)
		{
			while(enumKey.hasMoreElements()) {
			    String key = enumKey.nextElement();
				JCheckBox checkBox = (JCheckBox) servicePanel.checkBoxHash.get(key);
				if (checkBox.isSelected())
				{
					filterServices = filterServices + "(<http://health.mil/ontologies/Concept/Service/" + checkBox.getText() + ">)";
					//if(enumKey.hasMoreElements())
						//filterServices = filterServices + ",";
				}
			}
		}
		Hashtable<String, String> hash = new Hashtable<String, String>();
		if(query==null) {
			Utility.showError("The selected RDF store does not contain the necessary information \nto " +
				"generate the requested report.  Please select another financial database and try again.");
			return null;
		}
		else if(!check){
			Utility.showError("The selected RDF store does not contain the necessary calculated values to " +
				"generate the report.\nPlease select a different RDF store or try running the calculations above.");
			return null;
		}
		hash.put("FILTER_VALUES", filterServices);
		query = Utility.fillParam(query, hash);
		
		return query;

		//if query is still null, wrong db was selected.  Time to show error message

		//if query is not null, good to go.  Time to run the algorithm.
	}
	
	/**
	 * Gets sustainment data about a particular system.
	 * @param system String
	
	 * @return String 	Sustainment information. */
	public String getSysSustainData (String system)
	{
		Hashtable paramHash = new Hashtable();
		paramHash.put("System", system);
		String query = "SELECT DISTINCT ?system ?GLitem ?LOE WHERE { {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SustainmentGLItem> ;} BIND(<http://semoss.org/ontologies/Relation/Influences> AS ?influences) BIND(<http://semoss.org/ontologies/Relation/Influences> AS ?influences2) {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?system <http://www.w3.org/2000/01/rdf-schema#label> \"@System@\" ;}{?system ?influences ?GLitem ;} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?LOE ;} }";

		String filledQuery = Utility.fillParam(query, paramHash);
		
		ArrayList <String[]> list = retListFromQuery(filledQuery);	
		String retString="";
		try
		{
			retString = ((String[])list.get(0))[2];
		}
		catch (RuntimeException e)
		{
			retString=null;
		}
		
		return retString;
	}
	
	/**
	 * Gets system specific data.
	 // TODO: This is the same as method below (getSysTrainingData)?
	 * @param system String
	
	 * @return Object[] */
	public Object[] getSysSemData (String system)
	{
		Hashtable paramHash = new Hashtable();
		paramHash.put("System", system);
		String query = "SELECT DISTINCT ?system ?GLitem ?date ?LOE WHERE { {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SemanticsGLItem> ;} BIND(<http://semoss.org/ontologies/Relation/Influences> AS ?influences)  BIND(<http://semoss.org/ontologies/Relation/Influences> AS ?influences2) {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?system <http://www.w3.org/2000/01/rdf-schema#label> \"@System@\" ;}{?system ?influences ?GLitem ;} BIND(<http://semoss.org/ontologies/Relation/TaggedBy> AS ?taggedby) {?GLitem ?taggedby ?datetag ;} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?LOE ;} {?datetag <http://semoss.org/ontologies/Relation/Contains/Date> ?date ;} }";

		String filledQuery = Utility.fillParam(query, paramHash);
		
		ArrayList <Object[]> list = retListFromQuery(filledQuery);
		
		Object[] retElements = new Object[2];
		try
		{
			retElements[0] = ((Object[])list.get(0))[2];
			retElements[1] = ((Object[])list.get(0))[3];
		}
		catch (RuntimeException e)
		{
			retElements=null;
		}
		return retElements;
	}
	
	/**
	 * Gets system training data - includes information about GL item, date, and LOE.
	 * @param system String
	
	 * @return Object[] 	List of returned elements from query. */
	public Object[] getSysTrainingData (String system)
	{
		Hashtable paramHash = new Hashtable();
		paramHash.put("System", system);
		String query = "SELECT DISTINCT ?system ?GLitem ?date ?LOE WHERE { {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TrainingGLItem> ;} BIND(<http://semoss.org/ontologies/Relation/Influences> AS ?influences) BIND(<http://semoss.org/ontologies/Relation/Influences> AS ?influences2) {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?system <http://www.w3.org/2000/01/rdf-schema#label> \"@System@\" ;}{?system ?influences ?GLitem ;} BIND(<http://semoss.org/ontologies/Relation/TaggedBy> AS ?taggedby) {?GLitem ?taggedby ?datetag ;} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?LOE ;} {?datetag <http://semoss.org/ontologies/Relation/Contains/Date> ?date ;} }";

		String filledQuery = Utility.fillParam(query, paramHash);
		
		ArrayList <Object[]> list = retListFromQuery(filledQuery);
		
		Object[] retElements = new Object[2];
		try
		{
			retElements[0] = ((Object[])list.get(0))[2];
			retElements[1] = ((Object[])list.get(0))[3];
		}
		catch (RuntimeException e)
		{
			retElements=null;
		}
		return retElements;
	}
	
	/**
	 * Gets system specific data given a system and a query.
	 * @param system String
	 * @param query String
	
	 * @return ArrayList 	List of data. */
	public ArrayList getSpecData(String system, String query)
	{
		ArrayList <Object[]> totalList = new ArrayList <Object[]>();
		Hashtable paramHash = new Hashtable();
		paramHash.put("System", system);
		String filledQuery = Utility.fillParam(query, paramHash);
		ArrayList <Object[]> list = retListFromQuery(filledQuery);
		
		
		return list;
	}
	/**
	 * Return column names given a certain query.
	 * @param query String
	
	 * @return String[]	List of column names. */
	public String[] retColumnsFromQuery (String query)
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

		String[] names = wrapper.getVariables();
		return names;
	}
	/**
	 * Returns a list from a query.
	 * @param query String
	
	 * @return ArrayList		List of counts and values. */
	public ArrayList retListFromQuery (String query)
	{
		ArrayList <Object []> list = new ArrayList();
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
				
				Object [] values = new Object[names.length];
				boolean filledData = true;
				for(int colIndex = 0;colIndex < names.length;colIndex++)
				{
					if(sjss.getVar(names[colIndex]) != null)
					{
						values[colIndex] = sjss.getVar(names[colIndex]);
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
			System.out.println("ignored");
		}
		
		return list;
	}
	
	/**
	 * Gets the start date.
	 * @param highestLOESet 	Object[] List of highest LOE set.
	 * @param preEndDate 		Previous end date (Date)
	 * @param loeIdx 			LOE index (int)
	 * @param dateIdx 			Date index (int)
	
	 * @return Date 			Start date. */
	public Date getStartDate (Object[] highestLOESet,Date preEndDate, int loeIdx, int dateIdx)
	{
		String scheduledDateStr = (String) highestLOESet[dateIdx];
		Date scheduledDate = null;
		try {
			scheduledDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.ENGLISH).parse(scheduledDateStr);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		if (preEndDate!= null && preEndDate.after(scheduledDate))
		{
			scheduledDate = preEndDate;
		}

		return scheduledDate;
	}
	
	/**
	 * Get the end date of the service.
	 * @param highestLOESet 	Object[] List of LOEs.
	 * @param startDate 		Start date.
	 * @param loeIdx 			LOE index (int).
	 * @param dateIdx 			Date index (int).
	
	 * @return Date 			End date. */
	public Date getEndDate (Object[] highestLOESet, Date startDate, int loeIdx, int dateIdx)
	{
		Date date = null;
		double highestLOE = (Double) highestLOESet[loeIdx];
		Integer workDays = (int) Math.ceil(highestLOE/8);
		Integer totalDays = (int) (workDays + Math.floor(workDays/5)*2);
		Calendar cal = Calendar.getInstance();
		cal.setTime(startDate);
		cal.add(Calendar.DATE, totalDays);
		date = cal.getTime();
		return date;
	}
}
