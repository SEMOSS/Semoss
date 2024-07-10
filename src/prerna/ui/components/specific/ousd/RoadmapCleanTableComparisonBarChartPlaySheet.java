//package prerna.ui.components.specific.ousd;
//
//import java.lang.reflect.InvocationTargetException;
//import java.util.ArrayList;
//import java.util.Hashtable;
//import java.util.Iterator;
//import java.util.List;
//
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
//
//import prerna.algorithm.api.ITableDataFrame;
//import prerna.ds.h2.H2Frame;
//import prerna.engine.api.IHeadersDataRow;
//import prerna.ui.components.playsheets.BrowserPlaySheet;
//import prerna.util.Constants;
//import prerna.util.DIHelper;
//import prerna.util.PlaySheetRDFMapBasedEnum;
//
//public class RoadmapCleanTableComparisonBarChartPlaySheet extends RoadmapCleanTableComparisonPlaySheet{
//
//	private static final Logger logger = LogManager.getLogger(RoadmapCleanTableComparisonBarChartPlaySheet.class.getName());
//	List<Object[]> myList = new ArrayList<Object[]>();
//	
//	@Override
//	public void createView(){
//		String playSheetClassName = PlaySheetRDFMapBasedEnum.getClassFromName("Column");
//		BrowserPlaySheet playSheet = null;
//		try {
//			playSheet = (BrowserPlaySheet) Class.forName(playSheetClassName).getConstructor(null).newInstance(null);
//		} catch (ClassNotFoundException ex) {
//			ex.printStackTrace();
//			logger.fatal("No such PlaySheet: "+ playSheetClassName);
//		} catch (InstantiationException e) {
//			classLogger.error(Constants.STACKTRACE, e);
//			logger.fatal("No such PlaySheet: "+ playSheetClassName);
//		} catch (IllegalAccessException e) {
//			logger.fatal("No such PlaySheet: "+ playSheetClassName);
//			classLogger.error(Constants.STACKTRACE, e);
//		} catch (IllegalArgumentException e) {
//			logger.fatal("No such PlaySheet: "+ playSheetClassName);
//			classLogger.error(Constants.STACKTRACE, e);
//		} catch (InvocationTargetException e) {
//			logger.fatal("No such PlaySheet: "+ playSheetClassName);
//			classLogger.error(Constants.STACKTRACE, e);
//		} catch (NoSuchMethodException e) {
//			logger.fatal("No such PlaySheet: "+ playSheetClassName);
//			classLogger.error(Constants.STACKTRACE, e);
//		} catch (SecurityException e) {
//			logger.fatal("No such PlaySheet: "+ playSheetClassName);
//			classLogger.error(Constants.STACKTRACE, e);
//		}
//		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
//		playSheet.setDataMaker(this.dataFrame);
//		playSheet.setQuestionID(this.questionNum);
//		playSheet.setTitle(this.title);
//		playSheet.pane = this.pane;
//		playSheet.setDataMaker(this.dataFrame);
//		playSheet.processQueryData();
//		playSheet.createView();
//	}
//
//	@Override
//	public Hashtable getDataMakerOutput(String... selectors){
//		Hashtable ret = OUSDPlaysheetHelper.getData(this.title, this.questionNum, this.dataFrame, PlaySheetRDFMapBasedEnum.getSheetName("Column"));
//		return ret;
//	}
//	
//	@Override
//	public void createData(){
//		
//		buildTable(timelineNames, null);
//		
//		// go through the final table and reformat to how I need
//		// we want a row for each fy
//		// FY; Annual Savings; Annual Expenses; Annual Cash Flow; Cumulative Net Savings
//		String roadmapNewSavingsString = timelineNames.get(0) + this.savingThisYear;
//		String compRoadmapNewSavingsString = timelineNames.get(1) + this.savingThisYear;
//		String[] newHeaders = new String[]{"Fiscal Year", roadmapNewSavingsString, compRoadmapNewSavingsString};
//		String[] prevHeaders = this.getNames();
//		
//		// get the two rows we are interested in
//		Object[] roadmapNewSavings = null;
//		Object[] compRoadmapNewSavings = null;
//		Iterator<IHeadersDataRow> tableIt = this.dataFrame.iterator();
//		while(tableIt.hasNext()){
//			Object[] row = tableIt.next().getValues();
//			String rowName = row[0] + "";
//			if(rowName.equals(roadmapNewSavingsString)){
//				roadmapNewSavings = row;
//			}
//			else if(rowName.equals(compRoadmapNewSavingsString)){
//				compRoadmapNewSavings = row;
//			}
//		}
//		
//		ITableDataFrame newFrame = new H2Frame(newHeaders);
//		for(int fyIdx = 1; fyIdx < prevHeaders.length; fyIdx++){
//
//			Object[] newRow = new Object[newHeaders.length];
//			newRow[0] = prevHeaders[fyIdx];
//			newRow[1] = roadmapNewSavings[fyIdx] == null || roadmapNewSavings[fyIdx].toString().isEmpty() ? 0.0 : Double.parseDouble(roadmapNewSavings[fyIdx].toString().replace("$", "").replace(",", ""));
//			newRow[2] = compRoadmapNewSavings[fyIdx] == null || compRoadmapNewSavings[fyIdx].toString().isEmpty() ? 0.0 : Double.parseDouble(compRoadmapNewSavings[fyIdx].toString().replace("$", "").replace(",", ""));
//			newFrame.addRow(newRow);
//		}
//		this.dataFrame = newFrame;
//	}
//}
