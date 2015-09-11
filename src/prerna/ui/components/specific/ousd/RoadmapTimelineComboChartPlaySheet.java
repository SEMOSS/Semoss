package prerna.ui.components.specific.ousd;

import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.BTreeDataFrame;
import prerna.ui.components.playsheets.BasicProcessingPlaySheet;
import prerna.ui.components.playsheets.BrowserPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.PlaySheetEnum;


public class RoadmapTimelineComboChartPlaySheet extends RoadmapTimelineStatsPlaySheet {

	private static final Logger logger = LogManager.getLogger(RoadmapTimelineComboChartPlaySheet.class.getName());

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
		playSheet.processQueryData();
		playSheet.createView();
	}
	
	@Override
	public void createData(){
		super.createData();
		
		// go through the final table and reformat to how I need
		// we want a row for each fy
		// FY; Annual Savings; Annual Expenses; Annual Cash Flow; Cumulative Net Savings
		String[] prevHeaders = this.getNames();
		String[] newHeaders = new String[]{"Fiscal Year", "Annual Savings", "Annual Expenses", "Annual Cash Flow", "Cumulative Net Savings"};
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
						annlExpenses = annlExpenses + Double.parseDouble(value.toString().replace("$", "").replace(",", ""));
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
		}
		this.dataFrame = newFrame;
	}
}
