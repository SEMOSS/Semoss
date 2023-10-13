package prerna.reactor.map;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;

public abstract class AbstractMapReactor extends AbstractReactor {

	protected Object getValue(NounMetadata noun) {
		PixelDataType nounType = noun.getNounType();
		if(nounType == PixelDataType.TASK) {
			ITask task = (ITask) noun.getValue();
			// iterate through the task to get the table data
			List<Object[]> data = task.flushOutIteratorAsGrid();
			return flushOutData(data);
		} else if(nounType == PixelDataType.FORMATTED_DATA_SET && noun.getValue() instanceof Map) {
			Map<String, Object> collectData = (Map<String, Object>) noun.getValue();
			if(collectData.containsKey("data")) {
				Map<String, Object> dataMap = (Map<String, Object>) collectData.get("data");
				Object values = dataMap.get("values");
				if(values != null && values instanceof List) {
					List<Object[]> data = (List<Object[]>) values;
					return flushOutData(data);
				}
			}
		}

		return noun.getValue();
	}
	
	/**
	 * Flush out the data
	 * @param data
	 * @return
	 */
	private List flushOutData(List<Object[]> data) {
		int size = data.size();
		List flushedOutCol = null;
		if(size > 0) {
			// see if we flush one or multiple columns
			// to see if we flush out a column
			// or just return as is
			boolean multi = data.get(0).length > 1;
			if(multi) {
				flushedOutCol = data;
			} else {
				flushedOutCol = new ArrayList<Object>(size);
				for(int i = 0; i < size; i++) {
					flushedOutCol.add(data.get(i)[0]);
				}
			}
		} else {
			flushedOutCol = new ArrayList<Object>(size);
		}
		return flushedOutCol;
	}
	
	@Override
	public void mergeUp() {
		// while this is a reactor
		// it is basically a scalar that we will merge up
		if(parentReactor != null) {
			NounMetadata data = execute();
			this.parentReactor.getCurRow().add(data);
		}
	}
	
}
