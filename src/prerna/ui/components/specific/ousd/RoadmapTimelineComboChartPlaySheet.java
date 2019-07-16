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

import prerna.ds.rdbms.h2.H2Frame;
import prerna.engine.api.IHeadersDataRow;
import prerna.ui.components.playsheets.BrowserPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.PlaySheetRDFMapBasedEnum;


public class RoadmapTimelineComboChartPlaySheet extends RoadmapTimelineStatsPlaySheet {

	private static final Logger logger = LogManager.getLogger(RoadmapTimelineComboChartPlaySheet.class.getName());
	protected Hashtable dataHash = new Hashtable();
	

	public void processThickQueryData()
	{		
		ArrayList< ArrayList<Hashtable<String, Object>>> dataObj = new ArrayList< ArrayList<Hashtable<String, Object>>>();
		String[] names = dataFrame.getColumnHeaders();
		String[] newHeaders = this.timelines.get(0).getCostSavingsHeaders();
		//series name - all objects in that series (x : ... , y : ...)
//		Iterator<Object[]> it = dataFrame.iterator(true);
		Iterator<IHeadersDataRow> it = this.dataFrame.iterator();
		List<Object> annlSavingsSeries = new ArrayList<Object>();
		annlSavingsSeries.add(newHeaders[1]);
		List<Object> annlExpensesSeries = new ArrayList<Object>();
		annlExpensesSeries.add(newHeaders[2]);
		List<Object> annlCashFlowSeries = new ArrayList<Object>();
		annlCashFlowSeries.add(newHeaders[3]);
		List<Object> cumNetSavingsSeries = new ArrayList<Object>();
		cumNetSavingsSeries.add(newHeaders[4]);
		List<Object> ticks = new ArrayList<Object>();
		while(it.hasNext())
		{
			Object[] elemValues = it.next().getValues();
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
		types.put(newHeaders[4], "line");
		
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
		processThickQueryData();
		String playSheetClassName = PlaySheetRDFMapBasedEnum.getClassFromName("Column");
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
		playSheet.setDataMaker(this.dataFrame);
		playSheet.setQuestionID(this.questionNum);
		playSheet.setTitle(this.title);
		playSheet.pane = this.pane;
		playSheet.setDataHash(dataHash);
		playSheet.createView();
	}

	@Override
	public Hashtable getDataMakerOutput(String... selectors){
		Hashtable ret = OUSDPlaysheetHelper.getData(this.title, this.questionNum, this.dataFrame, PlaySheetRDFMapBasedEnum.getSheetName("Column"));
		
		ret.put("layout", "OUSDCombo");
		return ret;
	}
	
	@Override
	public void createData(){
		buildTable(timelineNames, null);
		OUSDTimeline time = this.timelines.get(0);
		List<Object[]> data = time.getCostSavingsData();
		this.dataFrame = new H2Frame(time.getCostSavingsHeaders());
		for(Object[] row : data){
			this.dataFrame.addRow(row, time.getCostSavingsHeaders());
		}
	}
}
