//package prerna.ui.components.specific.ousd;
//
//import java.lang.reflect.InvocationTargetException;
//import java.util.Hashtable;
//import java.util.List;
//
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
//
//import prerna.ds.h2.H2Frame;
//import prerna.ui.components.playsheets.GridPlaySheet;
//import prerna.util.PlaySheetRDFMapBasedEnum;
//
//public class ActivitySystemGroupPlaySheet extends GridPlaySheet{
//
//	private static final Logger LOGGER = LogManager.getLogger(SequencingDecommissioningPlaySheet.class.getName());
//
//	String insightNameOne;
//	String insightNameTwo;
//
//	public ActivitySystemGroupPlaySheet(){
//		super();
//	}
//
//	/* (non-Javadoc)
//	 * @see prerna.ui.components.playsheets.AbstractRDFPlaySheet#setQuery(java.lang.String)
//	 */
//	@Override
//	public void setQuery(String query){
//		String delimiters = "[,]";
//		String[] insights = query.split(delimiters);
//		insightNameOne = insights[0];
//		insightNameTwo = insights[1];
//	}
//	
//	/* (non-Javadoc)
//	 * @see prerna.ui.components.playsheets.BasicProcessingPlaySheet#createData()
//	 */
//	@Override
//	public void createData(){
////		ExecuteQueryProcessor proc = new ExecuteQueryProcessor();
////		Hashtable<String, Object> emptyTable = new Hashtable<String, Object>();
////		proc.processQuestionQuery(this.engine, insightNameOne, emptyTable);
//		SequencingDecommissioningPlaySheet activitySheet = (SequencingDecommissioningPlaySheet) OUSDPlaysheetHelper.getPlaySheetFromName(insightNameOne, engine);
//		//createData makes the table...
//		activitySheet.createData();
//		List<Object[]> actTable = activitySheet.getList();
//		String[] actNames = activitySheet.getNames();
//
////		proc.processQuestionQuery(this.engine, insightNameTwo, emptyTable);
//		SequencingDecommissioningPlaySheet systemSheet = (SequencingDecommissioningPlaySheet) OUSDPlaysheetHelper.getPlaySheetFromName(insightNameTwo, engine);
//		systemSheet.createData();
//		List<Object[]> systemTable = systemSheet.getList();
//		String[] sysNames = systemSheet.getNames();
//
//		combineTables(actTable, systemTable, actNames, sysNames);
//	}
//
//	/**
//	 * @param actTable
//	 */
//	private void combineTables(List<Object[]> activities, List<Object[]> systems, String[] actNames, String[] sysNames){
//
//		String[] updatedNames = new String[actNames.length + sysNames.length - 1];
//        int sysMatchIdx = 0;
//        int actMatchIdx = 0;
//        activity: for(int i = 0; i < actNames.length; i++) {
//               String actName = actNames[i];
//               for(int x = 0; x < sysNames.length; x ++ ){
//            	   String sysName = sysNames[x];
//            	   if(sysName.equals(actName)){
//            		   LOGGER.info("FOUND MATCH : " + sysName);
//            		   sysMatchIdx = x;
//            		   actMatchIdx = i;
//            		   break activity;
//            	   }
//               }
//        }
//        int idx = 0;
//        for(int i = 0; i < actNames.length; i++){
//        	if(i!=actMatchIdx){
//        		updatedNames[idx] = actNames[i];
//        		idx++;
//        	}
//        }
//        for(String sysName : sysNames){
//        	updatedNames[idx] = sysName;
//        	idx++;
//        }
//
////		this.names = updatedNames;
////		this.list = new ArrayList<Object[]>();
//		this.dataFrame = new H2Frame(updatedNames);
//
//		for(Object[] row: activities){
//			Object[] newRow = new Object[8];
////			Map<String, Object> hashRow = new HashMap<String, Object>();
//			int colIdx = 0;
//	        for(; colIdx < actNames.length; colIdx++){
//				newRow[colIdx] = row[colIdx];
//	        }
//	        
//	        Object actSystem = row[actMatchIdx];
//	        if(actSystem != null && !actSystem.toString().isEmpty()){
//				for(Object[] system: systems){
//		        	Object sysSystem = system[sysMatchIdx];
//			        if(sysSystem != null && !sysSystem.toString().isEmpty()){
//			        	if(sysSystem.toString().equals(actSystem.toString())){
//			        		for(int x = 0; x < sysNames.length; x++){
//			    	        	if(x!=sysMatchIdx){
//			    	        		newRow[colIdx+x-1] = system[x];
//			    	        	}
//			        		}
//			        		break;
//			        	}
//					}							
//				}
//			}
//	        this.dataFrame.addRow(newRow, updatedNames);
//		}
//	}
//	
//	@Override
//	public Hashtable getDataMakerOutput(String... selectors){
//		String playSheetClassName = PlaySheetRDFMapBasedEnum.getClassFromName("Grid");
//		GridPlaySheet playSheet = null;
//		try {
//			playSheet = (GridPlaySheet) Class.forName(playSheetClassName).getConstructor(null).newInstance(null);
//		} catch (ClassNotFoundException ex) {
//			ex.printStackTrace();
//			LOGGER.fatal("No such PlaySheet: "+ playSheetClassName);
//		} catch (InstantiationException e) {
//			classLogger.error(Constants.STACKTRACE, e);
//			LOGGER.fatal("No such PlaySheet: "+ playSheetClassName);
//		} catch (IllegalAccessException e) {
//			LOGGER.fatal("No such PlaySheet: "+ playSheetClassName);
//			classLogger.error(Constants.STACKTRACE, e);
//		} catch (IllegalArgumentException e) {
//			LOGGER.fatal("No such PlaySheet: "+ playSheetClassName);
//			classLogger.error(Constants.STACKTRACE, e);
//		} catch (InvocationTargetException e) {
//			LOGGER.fatal("No such PlaySheet: "+ playSheetClassName);
//			classLogger.error(Constants.STACKTRACE, e);
//		} catch (NoSuchMethodException e) {
//			LOGGER.fatal("No such PlaySheet: "+ playSheetClassName);
//			classLogger.error(Constants.STACKTRACE, e);
//		} catch (SecurityException e) {
//			LOGGER.fatal("No such PlaySheet: "+ playSheetClassName);
//			classLogger.error(Constants.STACKTRACE, e);
//		}
//		playSheet.setTitle(this.title);
//		playSheet.setQuestionID(this.questionNum);//
//		Hashtable retHash = (Hashtable) playSheet.getDataMakerOutput();
//		List<Object[]> myList = this.getList();
//		List<Object[]> theList = myList.subList(0, 1000);
//		for(Object[] myRow : theList){
//			for(int i = 0; i < myRow.length; i++){
//				if(myRow[i] == null){
//					myRow[i] = "";
//				}
//				else {
//					myRow[i] = myRow[i].toString();
//				}
//			}
//		}
//		retHash.put("data", theList);
//		return retHash;
//	}
//
//}