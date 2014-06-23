package prerna.ui.components.specific.tap;

import java.awt.GridBagConstraints;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Vector;

import javax.swing.JPanel;

import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.GridScrollPane;
import prerna.ui.components.playsheets.GridPlaySheet;

/** 
 * Given two variables, this class creates X's in a table if there is a relationship between them.
 */
public class GenericXGridPlaySheet extends GridPlaySheet {
	
	@Override
	public void createData() {
		list = new ArrayList<Object[]>();
		list = processQuery(query);
	}

	public ArrayList<Object[]> processQuery(String queryString){
		ArrayList<Object[]> processedList = new ArrayList<Object[]>();

		logger.info("PROCESSING QUERY: " + queryString);
		
		//executes the query on a specified engine
		SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
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
			
			// variables 1 and 2 have a relationship between them if they are added to the processed list
			processedList.add(new Object[]{var1, var2});
			
			// row and column names for the table 
			if (!rowNames.contains(var1)) {
				rowNames.add(var1);
			}
			
			if (!colNames.contains(var2)) {
				colNames.add(var2);
			}
		}
		
		// convert the row and column arraylists to vectors
		// sort alphabetically and convert back to string arraylists		
		Vector <String> rowNamesAsVector = new Vector<String>(rowNames);
		Collections.sort(rowNamesAsVector);
		rowNames = new ArrayList<String>(rowNamesAsVector);

		Vector <String> colNamesAsVector = new Vector<String>(colNames);
		Collections.sort(colNamesAsVector);
		colNames = new ArrayList<String>(colNamesAsVector);
		
		// set the column names in the global names variable
		String[] colNamesArray = new String[colNames.size()+1];
		colNamesArray[0] = "";
		for (int i=0; i<colNames.size(); i++) {
			colNamesArray[i+1] = colNames.get(i);
		}
		names = colNamesArray;
		
		// create a matrix with row and column names
		// iterate through processed list, implement logic to create X's based on relationship
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
		
		// convert the matrix back into an arraylist
		ArrayList<Object[]> arrayList = new ArrayList<Object[]>(Arrays.asList(variableMatrix));
		arrayList.remove(0); 
		
		return arrayList;	
	}
	
	@Override
	public void addScrollPanel(JPanel mainPanel) {
		// adds horizontal scrolling functionality to display in SEMOSS
		GridScrollPane pane = new GridScrollPane(names, list);
		pane.addHorizontalScroll();
		
		GridBagConstraints gbc_scrollPane = new GridBagConstraints();
		gbc_scrollPane.fill = GridBagConstraints.BOTH;
		gbc_scrollPane.gridx = 0;
		gbc_scrollPane.gridy = 0;
		mainPanel.add(pane, gbc_scrollPane);
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
	}

}
