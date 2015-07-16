package prerna.ui.components.specific.ousd;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ds.BTreeDataFrame;
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
		List<Object[]> actTable = activitySheet.getDataFrame().getData();
		String[] names = activitySheet.getNames();

		proc.processQuestionQuery(this.engine, insightNameTwo, emptyTable);
		SequencingDecommissioningPlaySheet systemSheet = (SequencingDecommissioningPlaySheet) proc.getPlaySheet();
		systemSheet.createData();
		List<Object[]> systemTable = systemSheet.getDataFrame().getData();

		combineTables(actTable, systemTable, names);
	}

	/**
	 * @param actTable
	 */
	private void combineTables(List<Object[]> activities, List<Object[]> systems, String[] columnNames){

		String[] updatedNames = new String[6];
		updatedNames[0] = columnNames[0];
		updatedNames[1] = columnNames[1];
		updatedNames[2] = columnNames[2];
		updatedNames[3] = columnNames[3];
		updatedNames[4] = columnNames[4];
		updatedNames[5] = "System Group";

		this.dataFrame = new BTreeDataFrame(updatedNames);

		for(Object[] row: activities){
			Map<String, Object> hashRow = new HashMap<String, Object>();
			hashRow.put(updatedNames[0], row[0]);
			hashRow.put(updatedNames[1], row[1]);
			hashRow.put(updatedNames[2], row[2]);
			hashRow.put(updatedNames[3], row[3]);
			hashRow.put(updatedNames[4], row[4]);

			for(Object[] system: systems){
				if(row[3] != null){
					if(Utility.getInstanceName(row[3].toString()).equals(system[0].toString())){
						hashRow.put(updatedNames[5], system[1]);
						break;
					}else{
						hashRow.put(updatedNames[5], "");
					}							
				}
			}
			dataFrame.addRow(hashRow, hashRow);
		}
	}
}