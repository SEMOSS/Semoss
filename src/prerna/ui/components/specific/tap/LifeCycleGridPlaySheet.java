package prerna.ui.components.specific.tap;

import java.text.DateFormatSymbols;
import java.util.ArrayList;
import java.util.Calendar;

import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.playsheets.GridPlaySheet;

public class LifeCycleGridPlaySheet extends GridPlaySheet {

	private int year, month;

	@Override
	public void createData() {
		list = new ArrayList<Object[]>();
		list = processQuery(query);
	}

	public ArrayList<Object[]> processQuery(String queryString){
		ArrayList<Object[]> processedList = new ArrayList<Object[]>();

		logger.info("PROCESSING QUERY: " + queryString);
		SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		//run the query against the engine provided
		sjsw.setEngine(engine);
		sjsw.setQuery(queryString);
		sjsw.executeQuery();

		names = sjsw.getVariables();

		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();

			String sys = sjss.getVar(names[0]).toString();
			String ver = sjss.getVar(names[1]).toString();
			String date = sjss.getVar(names[2]).toString();
			String obj = sjss.getVar(names[3]).toString();
			obj = obj.replace("\"", "");
			String price = sjss.getVar(names[4]).toString();
			String quantity = sjss.getVar(names[5]).toString();
			String cost = sjss.getVar(names[6]).toString();
			cost = cost.replace("\"", "");
			
			
			if(obj.equals("TBD"))
			{
				// only display cost if retired or sunset
				cost = "";
			}
			else
			{
				int lifecycleYear = Integer.parseInt(obj.substring(0,4));
				int lifecycleMonth = Integer.parseInt(obj.substring(5,7));
				
				if( (year > lifecycleYear) ||(year == lifecycleYear && month >= lifecycleMonth+6 ) || (year == lifecycleYear+1 && month >= lifecycleMonth+6-12) )
				{
					obj = "Retired_(Not_Supported)";
				}
				else if(year >= lifecycleYear || (year == lifecycleYear+1 && month >= lifecycleMonth))
				{
					obj = "Sunset_(End_of_Life)";
				}
				else if(year >= lifecycleYear+2 || (year==lifecycleYear+3 && month >= lifecycleMonth))
				{
					obj = "Supported";
					cost = ""; 
				}
				else
				{
					obj = "GA_(Generally_Available)";
					cost = ""; 
				}
			}
			
			if(price == null)
			{
				price = "";
			}
			
			if(quantity == null)
			{
				quantity = "";
			}
			
			if(cost == null)
			{
				cost = "";
			}
			
			if(date == null)
			{
				date = "";
			}
			processedList.add(new Object[]{sys, ver, date, obj, price, quantity, cost});
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
			int semicolon1 = query.indexOf(";");
			int semicolon2 = query.indexOf(";",semicolon1+1);
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
