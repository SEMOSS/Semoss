package prerna.sablecc2.reactor.task.lambda.map.function;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.sablecc2.om.task.AbstractTaskOperation;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.reactor.task.TaskBuilderReactor;

public class ApplyFormattingTaskReactor extends TaskBuilderReactor {
	
	private Map<String, String> colIndexFormatMap;
	private DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private DateFormat stf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public ApplyFormattingTaskReactor() {
		this.keysToGet = new String[]{};
	}
	
	@Override
	protected void buildTask() {
		if(this.task instanceof BasicIteratorTask) {
			SelectQueryStruct qs = ((BasicIteratorTask) task).getQueryStruct();
			ITableDataFrame frame = qs.getFrame();
			Map<String, String> additionalDataTypeMap = frame.getMetaData().getHeaderToAdtlTypeMap();
			
			if (!additionalDataTypeMap.isEmpty()) {
				//populate the colIndexFormatMap, which will indicate the column index and the format requested for a column
				Map<String, SemossDataType> dataTypeMap = frame.getMetaData().getHeaderToTypeMap();
				List<IQuerySelector> selectors = qs.getSelectors();
				populateColIndexFormatMap(selectors, additionalDataTypeMap, dataTypeMap, frame.getName());
				
				//create a new iterator task and attach it to the previous iterator
				ApplyFormattingTask newTask = new ApplyFormattingTask();
				newTask.setInnerTask(this.task);
				newTask.setApplyFormattingReactor(this);
				this.task = newTask;
				this.insight.getTaskStore().addTask(this.task);
			}
		}
	}
	
	private void populateColIndexFormatMap(List<IQuerySelector> selectors, Map<String, String> additionalDataTypeMap, 
		Map<String, SemossDataType> dataTypeMap, String frame){
		Map<String,String> map = new HashMap<String,String>();
		for (IQuerySelector s : selectors){
			String col = s.getQueryStructName();
			if (!col.contains("__")) {
				col = frame + "__" + col; 
			}
			if (additionalDataTypeMap.containsKey(col)) {
				int indx = selectors.indexOf(s);
				String type = dataTypeMap.get(col).toString();
				String format = additionalDataTypeMap.get(col);
				map.put(indx + "|" + type, format);
			}
		}
		this.colIndexFormatMap = map;
	}
	
	
	public IHeadersDataRow process(IHeadersDataRow row) {
		String[] headers = row.getHeaders();
		Object[] values = row.getValues(); 
		for (String key : this.colIndexFormatMap.keySet()){
			int pos = Integer.parseInt(key.split("\\|")[0]);
			String type = key.split("\\|")[1];
			String val = values[pos].toString();
			
			try {
				//convert the value to a java Date
				Date valAsDateTime = null;
				if (type.equals("DATE")) {
					valAsDateTime = sdf.parse(val);
				} else if (type.equals("TIMESTAMP")){
					valAsDateTime = stf.parse(val);
				}
				//format the date per requested format
				DateFormat df = new SimpleDateFormat(this.colIndexFormatMap.get(key));
				values[pos] = df.format(valAsDateTime);
			} catch (ParseException e1) {
				values[pos] = null;
			}
		}
		return new HeadersDataRow(headers, values);
	}
	
	public static void main(String[] args) throws ParseException {		
//		String dateColV = "2018-05-31";
//		DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//		Date d = sdf.parse(dateColV);
//		DateFormat df= new SimpleDateFormat("yyyy-MMM-dd");
//		System.out.println(df.format(d));
//		
//		String dTV = "2018-05-31 13:22:00";
//		DateFormat stf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//		Date t = stf.parse(dTV);
//		DateFormat tf= new SimpleDateFormat("yyyy/MM/dd'T'HH:mm:ss");
//		System.out.println(tf.format(t));
		
//		String tV = "13:22";
		String tV = "11:09:01.001";
		DateFormat stf1 = new SimpleDateFormat("HH:mm:ss.SSS"); //HH:mm:ss won't work
		Date t1 = stf1.parse(tV);
		DateFormat tf1= new SimpleDateFormat("HH:mm:ss:SSS");
		System.out.println(tf1.format(t1));	
	}
}



/**
 * Inner class to apply the formatting
 */
class ApplyFormattingTask extends AbstractTaskOperation {
	
	private ApplyFormattingTaskReactor applyFormattingReactor;
	
	@Override
	public IHeadersDataRow next() {
		IHeadersDataRow row = this.innerTask.next();
		return applyFormattingReactor.process(row);
	}
	
	public void setApplyFormattingReactor(ApplyFormattingTaskReactor applyFormattingReactor) {
		this.applyFormattingReactor = applyFormattingReactor;
	}
}


