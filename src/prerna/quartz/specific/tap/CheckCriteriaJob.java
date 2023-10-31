package prerna.quartz.specific.tap;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.google.gson.Gson;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IRawSelectWrapper;
import prerna.quartz.LinkedDataKeys;

public class CheckCriteriaJob implements org.quartz.Job {
	public static final String JSON_STRING = "JsonString";

	public static final String IN_DATA_FRAME_KEY = LinkedDataKeys.DATA_FRAME;
	public static final String IN_RETURN_COLUMN = LinkedDataKeys.RETURN_COLUMN;
	public static final String IN_COMPARE_COLUMN = LinkedDataKeys.COMPARE_COLUMN;
	public static final String IN_EVALUATED_EXPRESSION = LinkedDataKeys.EVALUATED_EXPRESSION;
	public static final String IN_COMPARATOR = LinkedDataKeys.COMPARATOR;
	public static final String IN_VALUE_TYPE = LinkedDataKeys.VALUE_TYPE;
	public static final String IN_VALUE = LinkedDataKeys.VALUE;

	public static final String OUT_IS_CRITERIA_MET_KEY = LinkedDataKeys.BOOLEAN;
	public static final String OUT_JSON_STRING_KEY = JSON_STRING;

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {

		// Get inputs
		JobDataMap dataMap = context.getMergedJobDataMap();
		ITableDataFrame results = (ITableDataFrame) dataMap.get(IN_DATA_FRAME_KEY);
		String returnColumn = dataMap.getString(IN_RETURN_COLUMN);
		String compareColumn = dataMap.getString(IN_COMPARE_COLUMN);
		String comparator = dataMap.getString(IN_COMPARATOR);
		String valueType = dataMap.getString(IN_VALUE_TYPE);
		String value = dataMap.getString(IN_VALUE);

		int instanceColumn = 1;

		// Do work
		// Print out the results
		// List<Object[]> resultsList = results.getData();
		List<Object[]> resultsList = new ArrayList<Object[]>();
		// TODO Parameterize
		IRawSelectWrapper iteratorResults = null;
		try {
			iteratorResults = results.query("SELECT " + returnColumn + ", " + compareColumn + " FROM " + results.getName());
			while (iteratorResults.hasNext()) {
				resultsList.add(iteratorResults.next().getRawValues());
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(iteratorResults != null) {
				try {
					iteratorResults.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
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

		List<Object[]> anomalyList = new ArrayList<Object[]>();

		if (valueType.equals("timeFrame")) {
			int intValue = Integer.parseInt(value);
			// get current time
			Date date = new Date(); // given date
			Calendar currentCalendar = GregorianCalendar.getInstance(); // creates
																		// a new
																		// calendar
																		// instance
			currentCalendar.setTime(date); // assigns calendar to given date
			// get one year before current time
			Calendar pastCalendar = currentCalendar;
			pastCalendar.add(Calendar.DAY_OF_YEAR, -intValue);

			String pattern = "yyyy-MM-dd HH:mm:ss.SSSSSS";
			Date instanceDate = null;

			for (Object[] row : resultsList) {
				String instanceValue = (String) row[instanceColumn];
				try {
					instanceDate = new SimpleDateFormat(pattern).parse(instanceValue);
					Calendar instanceCalendar = Calendar.getInstance();
					instanceCalendar.setTime(instanceDate);
					// TODO: only using after for testing purposes, need to
					// change to before
					if (instanceCalendar.after(pastCalendar)) {
						anomalyList.add(row);
					}
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}

		} else if (valueType.equals("double")) {
			Double doubleValue = Double.parseDouble(value);
			// Double doubleExpression =
			// Double.parseDouble(IN_EVALUATED_EXPRESSION);

			for (Object[] row : resultsList) {
				Double instanceValue = Double.parseDouble((String) row[instanceColumn]);
				int retval = Double.compare(instanceValue, doubleValue);

				if (comparator.equals("=") || comparator.equals("<=") || comparator.equals(">=")) {
					if (retval == 0) {
						anomalyList.add(row);
					}
				}

				if (comparator.equals("<") || comparator.equals("<=")) {
					if (retval < 0) {
						anomalyList.add(row);
					}
				}

				if (comparator.equals(">") || comparator.equals(">=")) {
					if (retval > 0) {
						anomalyList.add(row);
					}
				}
			}

		} else if (valueType.equals("string")) {
			// String query = "SELECT " + RETURN_COLUMN + "WHERE " +
			// COMPARE_COLUMN + " " + comparator + " " + VALUE;
			// run query, collect array of anomalies
			for (Object[] row : resultsList) {
				String instanceValue = (String) row[instanceColumn];
				if (instanceValue.equals(value)) {
					anomalyList.add(row);
				}
			}
		}

		boolean hasAnomaly = (anomalyList.size() > 0);
		if (hasAnomaly) {
			System.out.println("Threshold met");
		} else {
			System.out.println("Threshold not met");
		}

		// Store outputs
		Gson gson = new Gson();
		String jsonArrayString = gson.toJson(anomalyList);
		dataMap.put(OUT_IS_CRITERIA_MET_KEY, hasAnomaly);
		dataMap.put(OUT_JSON_STRING_KEY, jsonArrayString);
	}

}