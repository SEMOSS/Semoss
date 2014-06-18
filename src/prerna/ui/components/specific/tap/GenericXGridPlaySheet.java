package prerna.ui.components.specific.tap;

import java.awt.GridBagConstraints;
import java.text.DateFormatSymbols;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Vector;

import javax.swing.JPanel;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.GridScrollPane;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class GenericXGridPlaySheet extends GridPlaySheet {

	private int year, month;
	
	@Override
	public void createData() {
		list = new ArrayList<Object[]>();
		list = processQuery(query);
	}
	
	@Override
	public void addScrollPanel(JPanel mainPanel) {
		GridScrollPane pane = new GridScrollPane(names, list);
		pane.addHorizontalScroll();
		
		GridBagConstraints gbc_scrollPane = new GridBagConstraints();
		gbc_scrollPane.fill = GridBagConstraints.BOTH;
		gbc_scrollPane.gridx = 0;
		gbc_scrollPane.gridy = 0;
		mainPanel.add(pane, gbc_scrollPane);
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
		
		ArrayList<String> rowNames = new ArrayList<String>();
		ArrayList<String> colNames = new ArrayList<String>();

		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();

			String var1 = (String) sjss.getVar(names[0]);
			String var2 = (String) sjss.getVar(names[1]);
			
		processedList.add(new Object[]{var1, var2});
	
			if (!rowNames.contains(var1)) {
				rowNames.add(var1);
			}
			
			if (!colNames.contains(var2)) {
				colNames.add(var2);
			}
		}
		
		Vector <String> rowNamesAsVector = new Vector<String>(rowNames);
		Collections.sort(rowNamesAsVector);
		rowNames = new ArrayList<String>(rowNamesAsVector);

		Vector <String> colNamesAsVector = new Vector<String>(colNames);
		Collections.sort(colNamesAsVector);
		colNames = new ArrayList<String>(colNamesAsVector);
		
		String[] colNamesArray = new String[colNames.size()+1];
		colNamesArray[0] = "";
		for (int i=0; i<colNames.size(); i++) {
			colNamesArray[i+1] = colNames.get(i);
		}
		names = colNamesArray;
		
		String[][] variableMatrix = new String[rowNames.size()+1][colNames.size()+1];
		
		for (int i=0; i<rowNames.size(); i++) {
			variableMatrix[i+1][0] = rowNames.get(i);
		}
		
		for (int i=0; i<colNames.size(); i++) {
			variableMatrix[0][i+1] = colNames.get(i);
		}

		for (int i=0; i<processedList.size(); i++) {
			Object[] row = processedList.get(i);
			int rowInd = rowNames.indexOf(row[0])+1;
			int colInd = colNames.indexOf(row[1])+1;			
			variableMatrix[rowInd][colInd] = "X";
		}
		
		ArrayList<Object[]> arrayList = new ArrayList<Object[]>(Arrays.asList(variableMatrix));
		arrayList.remove(0);
		
		return arrayList;
		
		
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
