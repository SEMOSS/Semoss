package prerna.ui.components.specific.ousd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.components.ExecuteQueryProcessor;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.Utility;

public class ActivitySystemGroupPlaySheet extends GridPlaySheet{

	private static final Logger LOGGER = LogManager.getLogger(SequencingDecommissioningPlaySheet.class.getName());

	String insightNameOne;
	String insightNameTwo;

	public ActivitySystemGroupPlaySheet(){
		super();
	}

	/* (non-Javadoc)
	 * @see prerna.ui.components.playsheets.AbstractRDFPlaySheet#setQuery(java.lang.String)
	 */
	@Override
	public void setQuery(String query){
		String delimiters = "[,]";
		String[] insights = query.split(delimiters);
		insightNameOne = insights[0];
		insightNameTwo = insights[1];
	}
	
	/* (non-Javadoc)
	 * @see prerna.ui.components.playsheets.BasicProcessingPlaySheet#createData()
	 */
	@Override
	public void createData(){

		ExecuteQueryProcessor proc = new ExecuteQueryProcessor();
		Hashtable<String, Object> emptyTable = new Hashtable<String, Object>();
		proc.processQuestionQuery(this.engine, insightNameOne, emptyTable);
		SequencingDecommissioningPlaySheet activitySheet = (SequencingDecommissioningPlaySheet) proc.getPlaySheet();
		//createData makes the table...
		activitySheet.createData();
		ArrayList<Object[]> actTable = activitySheet.getList();
		String[] names = activitySheet.getNames();

		proc.processQuestionQuery(this.engine, insightNameTwo, emptyTable);
		SequencingDecommissioningPlaySheet systemSheet = (SequencingDecommissioningPlaySheet) proc.getPlaySheet();
		systemSheet.createData();
		ArrayList<Object[]> systemTable = systemSheet.getList();

		combineTables(actTable, systemTable, names);
	}

	/**
	 * @param actTable
	 */
	private void combineTables(ArrayList<Object[]> activities, ArrayList<Object[]> systems, String[] columnNames){

		String[] updatedNames = new String[6];
		updatedNames[0] = columnNames[0];
		updatedNames[1] = columnNames[1];
		updatedNames[2] = columnNames[2];
		updatedNames[3] = columnNames[3];
		updatedNames[4] = columnNames[4];
		updatedNames[5] = "System Group";
		this.names = updatedNames;

		list = new ArrayList<Object[]>();
		for(Object[] row: activities){
			Object[] newRow = new Object[6];
			newRow[0] = row[0];
			newRow[1] = row[1];
			newRow[2] = row[2];
			newRow[3] = row[3];
			newRow[4] = row[4];
			for(Object[] system: systems){
				if(newRow[3] != null){
					if(Utility.getInstanceName(newRow[3].toString()).equals(system[0].toString())){
						newRow[5] = system[1];
						break;
					}else{
						newRow[5] = "";
					}							
				}
			}
			list.add(newRow);
		}
	}
}