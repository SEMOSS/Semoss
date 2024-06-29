package prerna.quartz;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IRawSelectWrapper;
import prerna.util.Constants;

public class PrintDataToConsoleJob implements org.quartz.Job {

	public static final String IN_DATA_FRAME_KEY = CommonDataKeys.DATA_FRAME;
	private static final Logger classLogger = LogManager.getLogger(PrintDataToConsoleJob.class);

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {

		// Get inputs
		JobDataMap dataMap = context.getMergedJobDataMap();
		ITableDataFrame results = (ITableDataFrame) dataMap.get(IN_DATA_FRAME_KEY);

		// Do work
		List<Object[]> resultsList = new ArrayList<Object[]>();

		IRawSelectWrapper iteratorResults = null;
		try {
			iteratorResults = results.query("SELECT * FROM " + results.getName());
			while (iteratorResults.hasNext()) {
				resultsList.add(iteratorResults.next().getRawValues());
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if (iteratorResults != null) {
				try {
					iteratorResults.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		int length = 15;
		String[] headers = results.getColumnHeaders();
		System.out.print("|");
		for (String header : headers) {
			System.out.print(String.format("%-" + length + "." + (length - 2) + "s|", header));
		}
		System.out.println();
		for (Object[] row : resultsList) {
			System.out.print("|");
			for (Object element : row) {
				System.out.print(String.format("%-" + length + "." + (length - 2) + "s|", element));
			}
			System.out.println();
		}

		// Store outputs
		// No outputs to store here
	}

}
