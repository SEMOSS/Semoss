package prerna.rpa.quartz.jobs.insight;

import java.text.Collator;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.rpa.quartz.CommonDataKeys;

public class OneColConditionJob implements org.quartz.InterruptableJob {

	private static final Logger LOGGER = LogManager.getLogger(OneColConditionJob.class.getName());

	/** {@code ITableDataFrame} */
	public static final String IN_FRAME_KEY = CommonDataKeys.FRAME;
	
	/** {@code String} */
	public static final String IN_COLUMN_HEADER_KEY = OneColConditionJob.class + ".columnHeader";
	
	/** {@code Comparator} */
	public static final String IN_COMPARATOR_KEY = OneColConditionJob.class + ".comparator";
	
	/** {@code Object} */
	public static final String IN_VALUE_KEY = OneColConditionJob.class + ".value";
	
	/** {@code Set<IHeadersDataRow>} */
	public static final String OUT_ROWS_SATISFYING_CONDITION_KEY = CommonDataKeys.ROWS;
	
	private String jobName;
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {

		////////////////////
		// Get inputs
		////////////////////
		JobDataMap dataMap = context.getMergedJobDataMap();
		jobName = context.getJobDetail().getKey().getName();
		ITableDataFrame frame = (ITableDataFrame) dataMap.get(IN_FRAME_KEY);
		String columnHeader = dataMap.getString(IN_COLUMN_HEADER_KEY);
		Comparator comparator = (Comparator) dataMap.get(IN_COMPARATOR_KEY);
		Object value = dataMap.get(IN_VALUE_KEY);
		
		////////////////////
		// Do work
		////////////////////
		
		// Get the iterator
		String tableName = frame.getName();
		
		// Need this to preserve a logical order
		String[] headers = frame.getColumnHeaders();
		Iterator<IHeadersDataRow> iterator = frame.query("SELECT " + String.join(", ", headers) + " FROM " + tableName);
		
		// Get the index of the column header
		int index = Arrays.asList(headers).indexOf(columnHeader);
		
		// If the index is -1 (aka column doesn't exist), then throw an exception
		if (index == -1) {
			String noSuchColumnMessage = "Column " + columnHeader + " not found.";
			LOGGER.error(noSuchColumnMessage);
			throw new JobExecutionException(noSuchColumnMessage);
		}
		
		// Check whether the column is numeric
		Iterator<IHeadersDataRow> firstResultIterator = frame.query("SELECT " + String.join(", ", headers) + " FROM " + tableName + " LIMIT 1");
		boolean isNumeric = false;
		if (firstResultIterator.hasNext()) {
			try {
				Double.parseDouble(firstResultIterator.next().getValues()[index].toString());
				isNumeric = true;
			} catch (Exception e) {
				// Then assume is not numeric
			}
		}
		
		// TODO this isn't working for me
//		boolean isNumeric = frame.isNumeric()[index];
		
		Set<IHeadersDataRow> rows;
		// Comparison logic
		if (isNumeric) {
			double numericValue = Double.parseDouble(value.toString());
			
			// Switch on the comparator and use lambda functions to check the conditions
			switch (comparator) {
			case EQUALS:
				rows = IteratorLambdaFunctions.getRowsSatisfyingCondition(iterator, o -> Double.parseDouble(o.getValues()[index].toString()) == numericValue);
				break;
			case NOT_EQUALS:
				rows = IteratorLambdaFunctions.getRowsSatisfyingCondition(iterator, o -> Double.parseDouble(o.getValues()[index].toString()) != numericValue);
				break;
			case GREATER_THAN:
				rows = IteratorLambdaFunctions.getRowsSatisfyingCondition(iterator, o -> Double.parseDouble(o.getValues()[index].toString()) > numericValue);
				break;
			case LESS_THAN:
				rows = IteratorLambdaFunctions.getRowsSatisfyingCondition(iterator, o -> Double.parseDouble(o.getValues()[index].toString()) < numericValue);
				break;
			default:
				rows = new HashSet<IHeadersDataRow>();
			}
		} else {
			String stringValue = value.toString();
			 
			// Get the Collator for US English and set its strength to PRIMARY
			Collator collator = Collator.getInstance(Locale.US);
			collator.setStrength(Collator.PRIMARY);
			
			// Switch on the comparator and use lambda functions to check the conditions
			switch (comparator) {
			case EQUALS:
				rows = IteratorLambdaFunctions.getRowsSatisfyingCondition(iterator, o -> collator.compare(o.getValues()[index].toString(), stringValue) == 0);
				break;
			case NOT_EQUALS:
				rows = IteratorLambdaFunctions.getRowsSatisfyingCondition(iterator, o -> collator.compare(o.getValues()[index].toString(), stringValue) != 0);
				break;
			case GREATER_THAN:
				rows = IteratorLambdaFunctions.getRowsSatisfyingCondition(iterator, o -> collator.compare(o.getValues()[index].toString(), stringValue) > 0);
				break;
			case LESS_THAN:
				rows = IteratorLambdaFunctions.getRowsSatisfyingCondition(iterator, o -> collator.compare(o.getValues()[index].toString(), stringValue) < 0);
				break;
			default:
				rows = new HashSet<IHeadersDataRow>();
			}
		}

		////////////////////
		// Store outputs
		////////////////////
		dataMap.put(OUT_ROWS_SATISFYING_CONDITION_KEY, rows);
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		LOGGER.warn("Received request to interrupt the " + jobName + " job. However, there is nothing to interrupt for this job.");		
	}

}
