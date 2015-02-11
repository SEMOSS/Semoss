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

import java.text.DateFormatSymbols;
import java.util.ArrayList;
import java.util.Calendar;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.DIHelper;

@SuppressWarnings("serial")
public class LifeCycleGridPlaySheet extends GridPlaySheet {

	private static final Logger logger = LogManager.getLogger(LifeCycleGridPlaySheet.class.getName());
	private int year, month;

	@Override
	public void createData() {
		list = new ArrayList<Object[]>();
		list = processQuery(query);
	}

	public ArrayList<Object[]> processQuery(String queryString){
		ArrayList<Object[]> processedList = new ArrayList<Object[]>();

		logger.info("PROCESSING QUERY: " + queryString);
		
		//ISelectWrapper sjsw = WrapperManager.getInstance().getSWrapper(engine, queryString);
		
		SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		//run the query against the engine provided
		sjsw.setEngine(engine);
		sjsw.setQuery(queryString);
		sjsw.executeQuery();
		
		names = sjsw.getVariables();

		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();

			String sys = (String)sjss.getVar(names[0]);
			String ver = (String)sjss.getVar(names[1]);
			String date = (String)sjss.getVar(names[2]);
			String obj = (String)sjss.getVar(names[3]);
			obj = obj.replace("\"", "");
			Double price = ((Double)sjss.getVar(names[4]));
			Integer quantity = ((Double)sjss.getVar(names[5])).intValue();
			Double cost = ((Double)sjss.getVar(names[6]));
			Integer	budget = ((Double)sjss.getVar(names[7])).intValue();
			
			if(!obj.equals("TBD"))
			{
				int lifecycleYear = Integer.parseInt(obj.substring(0,4));
				int lifecycleMonth = Integer.parseInt(obj.substring(5,7));
				
				if( (year > lifecycleYear) ||(year == lifecycleYear && month >= lifecycleMonth+6 ) || (year == lifecycleYear+1 && month >= lifecycleMonth+6-12) )
					obj = "Retired_(Not_Supported)";
				else if(year >= lifecycleYear || (year == lifecycleYear+1 && month >= lifecycleMonth))
					obj = "Sunset_(End_of_Life)";
				else if(year >= lifecycleYear+2 || (year==lifecycleYear+3 && month >= lifecycleMonth))
					obj = "Supported";
				else
					obj = "GA_(Generally_Available)";
			}

			processedList.add(new Object[]{sys, ver, date, obj, price, quantity, cost,budget});
		}	
		
		try{
			IEngine tapCostEngine = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Cost_Data");
			String tapCostQuery = "SELECT DISTINCT ?System ?GLTag (max(coalesce(?FY15,0)) as ?fy15) WHERE { { {?SystemBudgetGLItem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemBudgetGLItem> ;} {?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}  {?GLTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag> ;}{?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;} {?FYTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag> ;} {?System ?Has ?SystemBudgetGLItem}{?SystemBudgetGLItem ?TaggedBy ?GLTag}{?SystemBudgetGLItem <http://semoss.org/ontologies/Relation/Contains/Cost> ?Budget ;} {?SystemBudgetGLItem ?OccursIn ?FYTag} {?OccursIn <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OccursIn>;} BIND(if(?FYTag = <http://health.mil/ontologies/Concept/FYTag/FY15>, ?Budget,0) as ?FY15)} UNION {{?SystemServiceBudgetGLItem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemServiceBudgetGLItem> ;} {?SystemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService> ;}{?ConsistsOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/ConsistsOf>;} {?System ?ConsistsOf ?SystemService}{?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}  {?GLTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag> ;}{?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;} {?FYTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag> ;} {?SystemService ?Has ?SystemServiceBudgetGLItem}{?SystemServiceBudgetGLItem ?TaggedBy ?GLTag}{?SystemServiceBudgetGLItem <http://semoss.org/ontologies/Relation/Contains/Cost> ?Budget ;} {?SystemServiceBudgetGLItem ?OccursIn ?FYTag} {?OccursIn <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OccursIn>;} BIND(if(?FYTag = <http://health.mil/ontologies/Concept/FYTag/FY15>, ?Budget,0) as ?FY15) } }GROUP BY ?System ?GLTag BINDINGS ?GLTag {(<http://health.mil/ontologies/Concept/GLTag/SW_Licen>) (<http://health.mil/ontologies/Concept/GLTag/HW>)}";
			sjsw = new SesameJenaSelectWrapper();
			//run the query against the engine provided
			sjsw.setEngine(tapCostEngine);
			sjsw.setQuery(tapCostQuery);
			sjsw.executeQuery();
			
			String[] names = sjsw.getVariables();

			while(sjsw.hasNext())
			{
				SesameJenaSelectStatement sjss = sjsw.next();

				String sys = (String)sjss.getVar(names[0]);
				String glTag = (String)sjss.getVar(names[1]);
				System.out.println(sjss.getVar(names[2]));
				Integer cost = ((Double)sjss.getVar(names[2])).intValue();
				String retVarString = queryString.substring(0,queryString.indexOf("WHERE")).toLowerCase();
				boolean isHardware = retVarString.contains("hardware")||retVarString.contains("hw");
				
				if((glTag.contains("HW")&&isHardware)||(glTag.contains("SW")&&!isHardware))
				{
					for(int i=0;i<processedList.size();i++)
					{
						if(processedList.get(i)[0].toString().equals(sys))
						{
							processedList.get(i)[7] = cost;
						}
					}
				}
				
			}
		} catch (RuntimeException e) {
			logger.error("Cannot find engine: TAP_Cost_Data");
			e.printStackTrace();
		}

		return processedList;
	}

	/**
	 * Sets the string version of the SPARQL query on the playsheet. 
	 * @param query String
	 */
	@Override
	public void setQuery(String query) 
	{
		if(query.startsWith("SELECT")||query.startsWith("CONSTRUCT"))
			this.query=query;
		else
		{
			logger.info("New Query " + query);
			int semicolon1 = query.indexOf("&");
			int semicolon2 = query.indexOf("&",semicolon1+1);
			Calendar now = Calendar.getInstance();
			year = now.get(Calendar.YEAR); //replace with current year
			if(!query.substring(0,semicolon1).equals("Today"))
				year = Integer.parseInt(query.substring(0,semicolon1));
			month = getIntForMonth(query.substring(semicolon1+1,semicolon2));
			if(month == -1)
				month= now.get(Calendar.MONTH);
			this.query = query.substring(semicolon2+1);
		}
	}

	/**
	 * Reads in a date format symbol and gets the months.
	 * The string form of the month is checked against the name of months and returns the integer form.
	 * @param m 		Month in string form.
	 * @return int		Month in integer form. */
	public int getIntForMonth(String m) {
		DateFormatSymbols dfs = new DateFormatSymbols();
		String[] months = dfs.getMonths();
		int i=0;
		while(i<12)
		{
			if(m.equals(months[i]))
				return i;
			i++;
		}
		return -1;
	}
}
