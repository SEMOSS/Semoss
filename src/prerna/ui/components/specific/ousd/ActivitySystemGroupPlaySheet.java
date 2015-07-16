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
		String[] actNames = activitySheet.getNames();

		proc.processQuestionQuery(this.engine, insightNameTwo, emptyTable);
		SequencingDecommissioningPlaySheet systemSheet = (SequencingDecommissioningPlaySheet) proc.getPlaySheet();
		systemSheet.createData();
		List<Object[]> systemTable = systemSheet.getDataFrame().getData();
		String[] sysNames = systemSheet.getNames();

		combineTables(actTable, systemTable, actNames, sysNames);
	}

	/**
	 * @param actTable
	 */
	private void combineTables(List<Object[]> activities, List<Object[]> systems, String[] actNames, String[] sysNames){

		String[] updatedNames = new String[actNames.length + sysNames.length - 1];
        int sysMatchIdx = 0;
        int actMatchIdx = 0;
        activity: for(int i = 0; i < actNames.length; i++) {
               String actName = actNames[i];
               for(int x = 0; x < sysNames.length; x ++ ){
            	   String sysName = sysNames[x];
            	   if(sysName.equals(actName)){
            		   LOGGER.info("FOUND MATCH : " + sysName);
            		   sysMatchIdx = x;
            		   actMatchIdx = i;
            		   break activity;
            	   }
               }
        }
        int idx = 0;
        for(int i = 0; i < actNames.length; i++){
        	if(i!=actMatchIdx){
        		updatedNames[idx] = actNames[i];
        		idx++;
        	}
        }
        for(String sysName : sysNames){
        	updatedNames[idx] = sysName;
        	idx++;
        }

		this.dataFrame = new BTreeDataFrame(updatedNames);

		for(Object[] row: activities){
			Map<String, Object> hashRow = new HashMap<String, Object>();
	        for(int i = 0; i < actNames.length; i++){
    			hashRow.put(actNames[i], row[i]);
	        }
	        
	        Object actSystem = row[actMatchIdx];
	        if(actSystem != null && !actSystem.toString().isEmpty()){
				for(Object[] system: systems){
		        	Object sysSystem = system[sysMatchIdx];
			        if(sysSystem != null && !sysSystem.toString().isEmpty()){
			        	if(sysSystem.toString().equals(actSystem.toString())){
			        		for(int x = 0; x < sysNames.length; x++){
			    	        	if(x!=sysMatchIdx){
			    	        		hashRow.put(sysNames[x], system[x]);
			    	        	}
			        		}
			        		break;
			        	}
					}							
				}
			}
			dataFrame.addRow(hashRow, hashRow);
		}
	}
}