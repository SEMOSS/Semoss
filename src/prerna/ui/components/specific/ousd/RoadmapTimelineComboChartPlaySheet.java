package prerna.ui.components.specific.ousd;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.annotations.BREAKOUT;
import prerna.ds.rdbms.h2.H2Frame;
import prerna.engine.api.IHeadersDataRow;
import prerna.ui.components.playsheets.BrowserPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.PlaySheetRDFMapBasedEnum;

@BREAKOUT
public class RoadmapTimelineComboChartPlaySheet extends RoadmapTimelineStatsPlaySheet {

	private static final Logger logger = LogManager.getLogger(RoadmapTimelineComboChartPlaySheet.class);

	private static final String STACKTRACE = "StackTrace: ";
	private static final String NO_PLAYSHEET = "No such PlaySheet: ";

	protected Hashtable dataHash = new Hashtable();

	public void processThickQueryData()
	{		
		ArrayList< ArrayList<Hashtable<String, Object>>> dataObj = new ArrayList<>();
		String[] names = dataFrame.getColumnHeaders();
		String[] newHeaders = this.timelines.get(0).getCostSavingsHeaders();
		//series name - all objects in that series (x : ... , y : ...)
//		Iterator<Object[]> it = dataFrame.iterator(true);
		Iterator<IHeadersDataRow> it = this.dataFrame.iterator();
		List<Object> annlSavingsSeries = new ArrayList<>();
		annlSavingsSeries.add(newHeaders[1]);
		List<Object> annlExpensesSeries = new ArrayList<>();
		annlExpensesSeries.add(newHeaders[2]);
		List<Object> annlCashFlowSeries = new ArrayList<>();
		annlCashFlowSeries.add(newHeaders[3]);
		List<Object> cumNetSavingsSeries = new ArrayList<>();
		cumNetSavingsSeries.add(newHeaders[4]);
		List<Object> ticks = new ArrayList<>();
		while(it.hasNext())
		{
			Object[] elemValues = it.next().getValues();
			ticks.add(elemValues[0]);
			
			annlSavingsSeries.add(elemValues[1]);
			
			annlExpensesSeries.add(elemValues[2]);
			
			annlCashFlowSeries.add(elemValues[3]);
			
			cumNetSavingsSeries.add(elemValues[4]);
			
		}
		List<List<Object>> columns = new ArrayList<>();
		columns.add(annlSavingsSeries);
		columns.add(annlExpensesSeries);
		columns.add(annlCashFlowSeries);
		columns.add(cumNetSavingsSeries);
		Map<String, Object> myHash = new HashMap<>();
		myHash.put("columns", columns);
		myHash.put("type", "bar");
		
		Map<String, String> types = new HashMap<>();
		types.put(newHeaders[4], "line");
		
		myHash.put("types", types);
//		myHash.put("groups", new Object[]{this.newHeaders[1], this.newHeaders[2], this.newHeaders[3]});
		
		
		Hashtable<String, Object> columnChartHash = new Hashtable<>();
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
			logger.error(STACKTRACE, ex);
			logger.fatal(NO_PLAYSHEET+ playSheetClassName);
		} catch (InstantiationException ie) {
			logger.error(STACKTRACE, ie);
			logger.fatal(NO_PLAYSHEET+ playSheetClassName);
		} catch (IllegalAccessException iae) {
			logger.error(STACKTRACE, iae);
			logger.fatal(NO_PLAYSHEET+ playSheetClassName);
		} catch (IllegalArgumentException iare) {
			logger.error(STACKTRACE, iare);
			logger.fatal(NO_PLAYSHEET+ playSheetClassName);
		} catch (InvocationTargetException ite) {
			logger.error(STACKTRACE, ite);
			logger.fatal(NO_PLAYSHEET+ playSheetClassName);
		} catch (NoSuchMethodException nsme) {
			logger.error(STACKTRACE, nsme);
			logger.fatal(NO_PLAYSHEET+ playSheetClassName);
		} catch (SecurityException se) {
			logger.error(STACKTRACE, se);
			logger.fatal(NO_PLAYSHEET+ playSheetClassName);
		}
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		if (playSheet != null) {
			playSheet.fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/ousd-combo.html";
			playSheet.setDataMaker(this.dataFrame);
			playSheet.setQuestionID(this.questionNum);
			playSheet.setTitle(this.title);
			playSheet.pane = this.pane;
			playSheet.setDataHash(dataHash);
			playSheet.createView();
		}
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
