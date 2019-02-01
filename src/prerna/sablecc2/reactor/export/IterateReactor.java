package prerna.sablecc2.reactor.export;

import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.InMemStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.reactor.AbstractReactor;

public class IterateReactor extends AbstractReactor {

	private static final String IN_MEM_STORE = "store";

	private BasicIteratorTask task;
	
	public IterateReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.QUERY_STRUCT.getKey(), ReactorKeysEnum.USE_FRAME_FILTERS.getKey()};
	}
	
	public NounMetadata execute() {
		return createJob();
	}
	
	private NounMetadata createJob()  {
		// the iterator is what creates a task
		// we need to take into consideration when we want to output
		// the following data sources
		
		// 1) data from an engine
		// 2) data from a frame
		// 3) data from a in-memory source
		
		// try to get a QS
		// ... not everything has a qs
		// ... primarily a key-value pair
		// ... TODO: should figure out a better way to bifurcate
		SelectQueryStruct qs = getQueryStruct();
		boolean useFrameFilters = useFrameFilters();
		
		// try to get an in memory store being used
		InMemStore inMemStore = getInMemoryStore();
		
		if(inMemStore != null) 
		{
			// TODO: figure out how to use a QS if present with this query
			IRawSelectWrapper iterator = inMemStore.getIterator();
			this.task = new BasicIteratorTask(iterator);
			this.insight.getTaskStore().addTask(this.task);
		} 
		else 
		{
			//TODO: add tableJoins to query
			//TODO: remove hard coded classes when we establish querystruct2 and sqlinterpreter2 function properly 
			//TODO: Hard coding this to use QueryStruct2 and SQLInterpreter2 to test changes

			// okay, we want to query an engine or a frame
			// do this based on if the key is defined in the QS
			IRawSelectWrapper iterator = null;
			if(qs.getQsType() == SelectQueryStruct.QUERY_STRUCT_TYPE.ENGINE ||
					qs.getQsType() == SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY) {
				iterator = WrapperManager.getInstance().getRawWrapper(qs.retrieveQueryStructEngine(), qs);
			} else {
				ITableDataFrame frame = qs.getFrame();
				if(frame == null) {
					frame = (ITableDataFrame) this.insight.getDataMaker();
				}
				if(useFrameFilters) {
					qs.mergeImplicitFilters(frame.getFrameFilters());
				}
				Logger logger = getLogger(frame.getClass().getName());
				frame.setLogger(logger);
				iterator = frame.query(qs);
			}
			this.task = new BasicIteratorTask(qs, iterator);
			this.task.setHeaderInfo(qs.getHeaderInfo());
			this.task.setSortInfo(qs.getSortInfo());
			this.task.setFilterInfo(qs.getExplicitFilters());
			this.insight.getTaskStore().addTask(this.task);
		}
		
		// create the return
		NounMetadata output = new NounMetadata(this.task, PixelDataType.TASK, PixelOperationType.TASK);
		output.setExplanation("Iterator created from iterate reactor");
		return output;
	}
	
	@Override
	public List<NounMetadata> getOutputs() {
		List<NounMetadata> outputs = super.getOutputs();
		if(outputs != null) return outputs;
		
		outputs = new Vector<NounMetadata>();
		NounMetadata output = new NounMetadata(this.task, PixelDataType.TASK, PixelOperationType.TASK);
		output.setExplanation("Iterator created from iterate reactor");
		outputs.add(output);
		return outputs;
	}

	/**
	 * Get the query struct that is defined 
	 * @return
	 */
	private SelectQueryStruct getQueryStruct() {
		GenRowStruct allNouns = getNounStore().getNoun(PixelDataType.QUERY_STRUCT.toString());
		SelectQueryStruct queryStruct = null;
		if(allNouns != null) {
			queryStruct = (SelectQueryStruct) allNouns.get(0);
		}
		return queryStruct;
	}
	
	private boolean useFrameFilters() {
		GenRowStruct grs = this.store.getNoun(keysToGet[1]);
		if(grs != null && !grs.isEmpty()) {
			return (boolean) grs.get(0);
		}
		
		return true;
	}
	
	private InMemStore getInMemoryStore() {
		InMemStore inMemStore = null;
		GenRowStruct grs = this.store.getNoun(this.IN_MEM_STORE);
		if(grs != null) {
			inMemStore = (InMemStore) grs.get(0);
		} else {
			grs = this.store.getNoun(PixelDataType.IN_MEM_STORE.toString());
			if(grs != null) {
				inMemStore = (InMemStore) grs.get(0);
			}
		}
		
		return inMemStore;
	}
}


