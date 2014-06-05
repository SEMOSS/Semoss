package prerna.ui.components.specific.tap;

import prerna.ui.components.playsheets.GridPlaySheet;

public class SystemPropertyGridPlaySheet  extends GridPlaySheet {

	//private String archiveUserResponse, interfaceUserResponse, probabilityUserResponse;
	
//	@Override
//	public void createData() {
//		list = new ArrayList<Object[]>();
//		list = processQuery(query);
//	}
//
//	public ArrayList<Object[]> processQuery(String queryString){
//		ArrayList<Object[]> processedList = new ArrayList<Object[]>();
//
//		logger.info("PROCESSING QUERY: " + queryString);
//		SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
//		//run the query against the engine provided
//		sjsw.setEngine(engine);
//		sjsw.setQuery(queryString);
//		sjsw.executeQuery();
//
//		names = sjsw.getVariables();
//
//		while(sjsw.hasNext())
//		{
//			SesameJenaSelectStatement sjss = sjsw.next();
//
//			String sub = sjss.getVar(names[0]).toString();
//			String pred = sjss.getVar(names[1]).toString();
//			String obj = sjss.getVar(names[2]).toString();
//			obj = obj.replace("\"", "");
//			String cost = sjss.getVar(names[3]).toString();
//			cost = cost.replace("\"", "");
//			
//			if(obj.equals("TBD"))
//			{
//				// do nothing
//			}
//			else
//			{
//				int lifecycleYear = Integer.parseInt(obj.substring(0,4));
//				int lifecycleMonth = Integer.parseInt(obj.substring(5,7));
//				
//				if( (year > lifecycleYear) ||(year == lifecycleYear && month >= lifecycleMonth+6 ) || (year == lifecycleYear+1 && month >= lifecycleMonth+6-12) )
//				{
//					obj = "Retired_(Not_Supported)";
//				}
//				else if(year >= lifecycleYear || (year == lifecycleYear+1 && month >= lifecycleMonth))
//				{
//					obj = "Sunset_(End_of_Life)";
//				}
//				else if(year >= lifecycleYear+2 || (year==lifecycleYear+3 && month >= lifecycleMonth))
//				{
//					obj = "Supported";
//				}
//				else
//				{
//					obj = "GA_(Generally_Available)";
//				}
//			}
//			
//			if(cost == null)
//			{
//				cost = "";
//			}
//			processedList.add(new Object[]{sub, pred, obj, cost});
//		}	
//		return processedList;
//	}

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
			logger.info("Query " + query);
			
			String addConditions = "";
			String addBindings = "";
			
			int semicolon1 = query.indexOf(";");
			int semicolon2 = query.indexOf(";",semicolon1+1);
			int semicolon3 = query.indexOf(" SELECT",semicolon2+1);
			
			String probabilityUserResponse = query.substring(0,semicolon1);
			String interfaceUserResponse = query.substring(semicolon1+1,semicolon2);
			String archiveUserResponse = query.substring(semicolon2+1, semicolon3);
			query = query.substring(semicolon3+1);
			
			if(probabilityUserResponse.equals("All"))
			{
				// do nothing
			} else {
				addConditions = addConditions.concat("OPTIONAL {?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability} ");
				if(probabilityUserResponse.equals("High"))
				{
					addBindings = addBindings.concat("BINDINGS ?Probability {('High')('Question')}");
				} else {
					addBindings = addBindings.concat("BINDINGS ?Probability {('Low')('Medium')('Medium-High')}");
				}
			}

			
			if(interfaceUserResponse.equals("All"))
			{
				// do nothing
			} else if(interfaceUserResponse.equals("Yes"))
			{
				addConditions = addConditions.concat("OPTIONAL {?System <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> 'Y' }");
			} else if(interfaceUserResponse.equals("No"))
			{
				addConditions = addConditions.concat("OPTIONAL {?System <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> 'N' }");
			}
			
			if(archiveUserResponse.equals("All"))
			{
				// do nothing
			} else if(archiveUserResponse.equals("Yes"))
			{
				addConditions = addConditions.concat("OPTIONAL {?System <http://semoss.org/ontologies/Relation/Contains/Archive_Req> 'Y' }");
			} else if(archiveUserResponse.equals("No"))
			{
				addConditions = addConditions.concat("OPTIONAL {?System <http://semoss.org/ontologies/Relation/Contains/Archive_Req> 'N' }");
			}
			
			String retString = "";
			retString = retString.concat(query.substring(0,query.lastIndexOf("}"))).concat(addConditions).concat("} ").concat(addBindings);
			logger.info("New Query " + retString);
			this.query = retString;
		}
	}
}
