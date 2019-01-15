package prerna.quartz.specific.anthem;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.quartz.CommonDataKeys;

public class DetermineIfAnomalyJob implements org.quartz.Job {

	public static final String IN_DATA_FRAME_KEY = CommonDataKeys.DATA_FRAME;

	public static final String OUT_IS_ANOMALY_KEY = CommonDataKeys.BOOLEAN;

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {

		// Get inputs
		JobDataMap dataMap = context.getMergedJobDataMap();
		ITableDataFrame results = (ITableDataFrame) dataMap.get(IN_DATA_FRAME_KEY);

		// Do work
		// Print out the results
//		List<Object[]> resultsList = results.getData();
		List<Object[]> resultsList = new ArrayList<Object[]>();
		// TODO Parameterize
		Iterator<IHeadersDataRow> iteratorResults = results.query("SELECT * FROM " + results.getName() + " ORDER BY Kickout_Date ASC");
		while (iteratorResults.hasNext()) {
			resultsList.add(iteratorResults.next().getRawValues());
		}
		
		int length = 30;
		String[] headers = results.getColumnHeaders();
		System.out.print("|");
		for (String header : headers) {
			System.out.print(String.format("%1$" + length + "s", header + "|"));
		}
		System.out.println();
		for (Object[] row : resultsList) {
			System.out.print("|");
			for (Object element : row) {
				System.out.print(String.format("%1$" + length + "s", element + "|"));
			}
			System.out.println();
		}

		// Determine whether the last observation was an anomaly
		Object[] lastRow = resultsList.get(resultsList.size() - 1);
		double anom = (double) lastRow[2];
		boolean isAnomaly = anom > 0;
		if (isAnomaly) {
			System.out.println("An anomaly was observed");
		} else {
			System.out.println("An anomaly was not observed");
		}
//		isAnomaly = true;

		// Store outputs
		dataMap.put(OUT_IS_ANOMALY_KEY, isAnomaly);
	}

}
