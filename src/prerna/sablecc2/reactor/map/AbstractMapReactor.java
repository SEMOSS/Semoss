package prerna.sablecc2.reactor.map;

import java.util.ArrayList;
import java.util.List;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.reactor.AbstractReactor;

public abstract class AbstractMapReactor extends AbstractReactor {

	protected Object getValue(NounMetadata noun) {
		PixelDataType nounType = noun.getNounType();
		if(nounType == PixelDataType.TASK) {
			ITask task = (ITask) noun.getValue();
			List<Object> flushedOutCol = new ArrayList<Object>();
			// iterate through the task to get the table data
			List<Object[]> data = task.flushOutIteratorAsGrid();
			int size = data.size();
			// assumes we are only flushing out the first column
			for(int i = 0; i < size; i++) {
				flushedOutCol.add(data.get(i)[0]);
			}
			
			return flushedOutCol;
		} else {
			return noun.getValue();
		}
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
