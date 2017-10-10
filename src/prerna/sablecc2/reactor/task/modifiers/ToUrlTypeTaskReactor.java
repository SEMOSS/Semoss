package prerna.sablecc2.reactor.task.modifiers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.task.AbstractTaskOperation;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.reactor.task.TaskBuilderReactor;

public class ToUrlTypeTaskReactor extends TaskBuilderReactor {

	private static final String COLUMNS_KEY = "columns";
	
	@Override
	protected void buildTask() {
		// take the inner task and use it within the new task
		ToUrlTypeIterator newTask = new ToUrlTypeIterator(this.task);
		// get the columns
		List<String> cols = getColumns();
		int numCols = cols.size();
		// figure out which indices are those we want to convert to a double
		List<Integer> indices = new ArrayList<Integer>();
		List<Map<String, Object>> headerInfo = this.task.getHeaderInfo();
		int totalCols = headerInfo.size();
		
		NEXT_COLUMN : for(int i = 0; i < numCols; i++) {
			String headerToConvert = cols.get(i);
			for(int j = 0; j < totalCols; j++) {
				Map<String, Object> headerMap = headerInfo.get(j);
				String alias = headerMap.get("alias").toString();
				if(alias.equals(headerToConvert)) {
					// add the index to convert
					// modify the type to double
					indices.add(new Integer(j));
					headerMap.put("type", "URL");
					continue NEXT_COLUMN;
				}
			}
		}
		newTask.setIndices(indices);
		
		// output values
		this.task = newTask;
		// also add this to the store!!!
		this.insight.getTaskStore().addTask(this.task);
	}
	
	private List<String> getColumns() {
		GenRowStruct colGrs = this.store.getNoun(COLUMNS_KEY);
		if(colGrs != null && !colGrs.isEmpty()) {
			int size = colGrs.size();
			List<String> columns = new ArrayList<String>();
			for(int i = 0; i < size; i++) {
				columns.add(colGrs.get(i).toString());
			}
			return columns;
		}
		
		List<String> columns = new ArrayList<String>();
		int size = this.curRow.size();
		for(int i = 0; i < size; i++) {
			columns.add(this.curRow.get(i).toString());
		}
		return columns;
	}
}

class ToUrlTypeIterator extends AbstractTaskOperation {

	private int numCols;
	private List<Integer> colIndices;

	public ToUrlTypeIterator(ITask innerTask) {
		super(innerTask);
	}

	@Override
	public boolean hasNext() {
		return this.innerTask.hasNext();
	}

	@Override
	public IHeadersDataRow next() {
		// as you iterate
		// just convert it to a Double
		IHeadersDataRow row = this.innerTask.next();
		String[] headers = row.getHeaders();
		Object[] values = row.getValues();
		for(int i = 0; i < numCols; i++) {
			int indexToGet = colIndices.get(i).intValue();
			// try to convert it
			values[indexToGet] = values[indexToGet].toString();
		}
		
		return new HeadersDataRow(headers, values);
	}
	
	public void setIndices(List<Integer> colIndices) {
		this.colIndices = colIndices;
		this.numCols = this.colIndices.size();
	}
}