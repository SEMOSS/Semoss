package prerna.ui.components.specific.ousd;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.BTreeDataFrame;
import prerna.ui.components.playsheets.BrowserPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.PlaySheetEnum;


public class RoadmapTimelineComboChartPlaySheet extends RoadmapTimelineStatsPlaySheet {

	private static final Logger logger = LogManager.getLogger(RoadmapTimelineComboChartPlaySheet.class.getName());
	String[] newHeaders = new String[]{"Fiscal Year", "Annual Savings", "Annual Expenses", "Annual Cash Flow", "Cumulative Net Savings"};
	List<Object[]> myList = new ArrayList<Object[]>();

	protected Hashtable dataHash = new Hashtable();
	

	public void processQueryData()
	{		
		ArrayList< ArrayList<Hashtable<String, Object>>> dataObj = new ArrayList< ArrayList<Hashtable<String, Object>>>();
		String[] names = dataFrame.getColumnHeaders();

		//series name - all objects in that series (x : ... , y : ...)
//		Iterator<Object[]> it = dataFrame.iterator(true);
		Iterator<Object[]> it = myList.iterator();
		List<Object> annlSavingsSeries = new ArrayList<Object>();
		annlSavingsSeries.add(this.newHeaders[1]);
		List<Object> annlExpensesSeries = new ArrayList<Object>();
		annlExpensesSeries.add(this.newHeaders[2]);
		List<Object> annlCashFlowSeries = new ArrayList<Object>();
		annlCashFlowSeries.add(this.newHeaders[3]);
		List<Object> cumNetSavingsSeries = new ArrayList<Object>();
		cumNetSavingsSeries.add(this.newHeaders[4]);
		List<Object> ticks = new ArrayList<Object>();
		while(it.hasNext())
		{
			Object[] elemValues = it.next();
			ticks.add(elemValues[0]);
			
			annlSavingsSeries.add(elemValues[1]);
			
			annlExpensesSeries.add(elemValues[2]);
			
			annlCashFlowSeries.add(elemValues[3]);
			
			cumNetSavingsSeries.add(elemValues[4]);
			
		}
		List<List<Object>> columns = new ArrayList<List<Object>>();
		columns.add(annlSavingsSeries);
		columns.add(annlExpensesSeries);
		columns.add(annlCashFlowSeries);
		columns.add(cumNetSavingsSeries);
		Map<String, Object> myHash = new HashMap<String, Object>();
		myHash.put("columns", columns);
		myHash.put("type", "bar");
		
		Map<String, String> types = new HashMap<String, String>();
		types.put(this.newHeaders[4], "line");
		
		myHash.put("types", types);
//		myHash.put("groups", new Object[]{this.newHeaders[1], this.newHeaders[2], this.newHeaders[3]});
		
		
		Hashtable<String, Object> columnChartHash = new Hashtable<String, Object>();
		columnChartHash.put("names", names);
		columnChartHash.put("data", myHash);
		columnChartHash.put("ticks", ticks);
		
		this.dataHash = columnChartHash;
	}
	
	@Override
	public void createView(){
		String playSheetClassName = PlaySheetEnum.getClassFromName("Column Chart");
		BrowserPlaySheet playSheet = null;
		try {
			playSheet = (BrowserPlaySheet) Class.forName(playSheetClassName).getConstructor(null).newInstance(null);
		} catch (ClassNotFoundException ex) {
			ex.printStackTrace();
			logger.fatal("No such PlaySheet: "+ playSheetClassName);
		} catch (InstantiationException e) {
			e.printStackTrace();
			logger.fatal("No such PlaySheet: "+ playSheetClassName);
		} catch (IllegalAccessException e) {
			logger.fatal("No such PlaySheet: "+ playSheetClassName);
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			logger.fatal("No such PlaySheet: "+ playSheetClassName);
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			logger.fatal("No such PlaySheet: "+ playSheetClassName);
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			logger.fatal("No such PlaySheet: "+ playSheetClassName);
			e.printStackTrace();
		} catch (SecurityException e) {
			logger.fatal("No such PlaySheet: "+ playSheetClassName);
			e.printStackTrace();
		}
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		playSheet.fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/ousd-combo.html";
		playSheet.setDataFrame(this.dataFrame);
		playSheet.setQuestionID(this.questionNum);
		playSheet.setTitle(this.title);
		playSheet.pane = this.pane;
		playSheet.setDataFrame(this.dataFrame);
		playSheet.setDataHash(dataHash);
		playSheet.createView();
	}
	
	@Override
	public void createData(){
		buildTable(timelineNames, null);
				
		// go through the final table and reformat to how I need
		// we want a row for each fy
		// FY; Annual Savings; Annual Expenses; Annual Cash Flow; Cumulative Net Savings
		String[] prevHeaders = this.getNames();
		ITableDataFrame newFrame = new BTreeDataFrame(newHeaders);
		Double cumNetSavings = 0.0;
		for(int fyIdx = 1; fyIdx < prevHeaders.length; fyIdx++){
			Double annlSavings = 0.0;
			Double annlExpenses = 0.0;

			Iterator<Object[]> tableIt = this.dataFrame.iterator(false);
			while(tableIt.hasNext()){
				Object[] row = tableIt.next();
				String rowName = row[0] + "";
				Object value = row[fyIdx];
				if(value!=null && !value.toString().isEmpty()){
					if (rowName.endsWith(this.savingThisYear) || rowName.endsWith(this.prevSavings)){
						annlSavings = annlSavings + Double.parseDouble(value.toString().replace("$", "").replace(",", ""));
					}
					if (rowName.endsWith(this.investmentCost) || rowName.endsWith(this.sustainCost)){
						annlExpenses = annlExpenses - Double.parseDouble(value.toString().replace("$", "").replace(",", ""));
					}
				}
			}
			Double annlCashFlow = annlSavings + annlExpenses;
			cumNetSavings = cumNetSavings + annlCashFlow;
			Object[] newRow = new Object[newHeaders.length];
			newRow[0] = prevHeaders[fyIdx];
			newRow[1] = annlSavings;
			newRow[2] = annlExpenses;
			newRow[3] = annlCashFlow;
			newRow[4] = cumNetSavings;
			newFrame.addRow(newRow, newRow);
			myList.add(newRow);
		}
		this.dataFrame = newFrame;
	}
}
