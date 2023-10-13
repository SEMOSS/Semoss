package prerna.reactor.task.modifiers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;
import prerna.reactor.task.TaskBuilderReactor;
import prerna.sablecc2.om.task.AbstractTaskOperation;
import prerna.sablecc2.om.task.ITask;

public class TransposeRowsReactor extends TaskBuilderReactor {

	@Override
	protected void buildTask() {
		// take the inner task and use it within the new task
		TransposeRowTaskIterator newTask = new TransposeRowTaskIterator(this.task);
		// create new header info
		List<Map<String, Object>> newHeaderInfo = new ArrayList<Map<String, Object>>();
		Map<String, Object> header1 = new HashMap<String, Object>();
		header1.put("alias", "Attribute");
		header1.put("derived", true);
		header1.put("type", "STRING");
		newHeaderInfo.add(header1);
		Map<String, Object> header2 = new HashMap<String, Object>();
		header2.put("alias", "Value");
		header2.put("derived", true);
		header2.put("type", determineTypes());
		newHeaderInfo.add(header2);
		newTask.setHeaderInfo(newHeaderInfo);
		// output values
		this.task = newTask;
		// also add this to the store!!!
		this.insight.getTaskStore().addTask(this.task);
	}
	
	private String determineTypes() {
		List<Map<String, Object>> curHeaderInfo = this.task.getHeaderInfo();
		int size = curHeaderInfo.size();
		String type = null;
		for(int i = 0; i < size; i++) {
			Map<String, Object> headerMap = curHeaderInfo.get(i);
			String curType = headerMap.get("type").toString();
			if(curType.equals("STRING")) {
				return "STRING";
			} else if(type == null){
				type = curType;
			} else if(type == curType) {
				// do nothing
			} else {
				// this means type and curType are not the same
				return "STRING";
			}
		}
		return type;
	}
}

class TransposeRowTaskIterator extends AbstractTaskOperation {

	private IHeadersDataRow row = null;
	private int curCol = 0;
	private String[] newHeaders = new String[]{"Attribute","Value"};
	
	public TransposeRowTaskIterator(ITask innerTask) {
		super(innerTask);
	}

	@Override
	public boolean hasNext() {
		if(this.row == null) {
			// not initialized
			// see if we have another row to get
			return this.innerTask.hasNext();
		} else {
			// see if the total number of records
			// is more than the current column index
			int totalCols = this.row.getRecordLength();
			if(totalCols > this.curCol) {
				return true;
			} else {
				return this.innerTask.hasNext();
			}
		}
	}

	@Override
	public IHeadersDataRow next() {
		if(this.row == null) {
			this.row = innerTask.next();
			this.curCol = 0;
		}
		Object[] values = new Object[2];
		values[0] = this.row.getHeaders()[this.curCol];
		values[1] = this.row.getValues()[this.curCol];
		
		// update to grab the next column value
		this.curCol++;
		// if we reached the end, set the row to null
		if(this.curCol == this.row.getRecordLength()) {
			this.row = null;
		}
		
		HeadersDataRow dataRow = new HeadersDataRow(this.newHeaders, values);
		return dataRow;
	}
}