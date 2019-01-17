package prerna.sablecc2.reactor.task;

import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.ConstantDataTask;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.reactor.AbstractReactor;

public abstract class TaskBuilderReactor extends AbstractReactor {

	private static final String CLASS_NAME = TaskBuilderReactor.class.getName();
	protected ITask task;
	protected List<NounMetadata> subAdditionalReturn;
	
	//This method is implemented by child classes, each class is responsible for building different pieces of the task
	protected abstract void buildTask();
	
	public NounMetadata execute() {
		this.task = getTask(); //initialize the task
		buildTask(); // append onto the task
		return new NounMetadata(this.task, PixelDataType.TASK); //return the task
	}
	
	@Override
	public List<NounMetadata> getOutputs() {
		// all of the classes return the same thing
		// which is a TASK
		// this works because even if execute hasn't occurred yet
		// because the same preference exists for the task
		// and since out is called prior to update the planner
		// the  cannot be null
		List<NounMetadata> outputs = new Vector<NounMetadata>();
		NounMetadata output = new NounMetadata(task, PixelDataType.TASK);
		outputs.add(output);
		return outputs;
	}
	
	/**
	 * Get the task for the task builder
	 * @return
	 */
	protected ITask getTask() {
		ITask task = null;
		
		GenRowStruct grsTasks = this.store.getNoun(PixelDataType.TASK.toString());
		//if we don't have jobs in the curRow, check if it exists in genrow under the key job
		if(grsTasks != null && !grsTasks.isEmpty()) {
			task = (ITask) grsTasks.get(0);
		} else {
			List<Object> tasks = this.curRow.getValuesOfType(PixelDataType.TASK);
			if(tasks != null && !tasks.isEmpty()) {
				task = (ITask) tasks.get(0);
			}
		}
		
		if(task == null) {
			task = constructTaskFromQs();
		}
		
		Logger logger = this.getLogger(CLASS_NAME);
		task.setLogger(logger);
		return task;
	}
	
	/**
	 * Generate the task from the query struct
	 * @return
	 */
	private ITask constructTaskFromQs() {
		NounMetadata noun = null;
		SelectQueryStruct qs = null;

		GenRowStruct grsQs = this.store.getNoun(PixelDataType.QUERY_STRUCT.toString());
		//if we don't have jobs in the curRow, check if it exists in genrow under the key job
		if(grsQs != null && !grsQs.isEmpty()) {
			noun = grsQs.getNoun(0);
			qs = (SelectQueryStruct) noun.getValue();
		} else {
			List<NounMetadata> qsList = this.curRow.getNounsOfType(PixelDataType.QUERY_STRUCT);
			if(qsList != null && !qsList.isEmpty()) {
				noun = qsList.get(0);
				qs = (SelectQueryStruct) noun.getValue();
			}
		}
		
		// no qs either... i guess we will return an empty constant data task
		// this will just store the information
		if(qs == null) {
			ConstantDataTask task = new ConstantDataTask();
			this.insight.getTaskStore().addTask(task);
			return task;
		}
		
		// set any additional details required
		this.subAdditionalReturn = noun.getAdditionalReturn();
		
		// handle some defaults
		QUERY_STRUCT_TYPE qsType = qs.getQsType();
		// first, do a basic check
		if(qsType != QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY && qsType != QUERY_STRUCT_TYPE.RAW_FRAME_QUERY) {
			// it is not a hard query
			// we need to make sure there is at least a selector
			if(qs.getSelectors().isEmpty()) {
				throw new IllegalArgumentException("There are no selectors in the query to return.  "
						+ "There must be at least one selector for the query to execute.");
			}
		}
		
		if(qsType == QUERY_STRUCT_TYPE.FRAME || qsType == QUERY_STRUCT_TYPE.RAW_FRAME_QUERY) {
			ITableDataFrame frame = qs.getFrame();
			if(frame == null) {
				frame = (ITableDataFrame) this.insight.getDataMaker();
			}
			qs.setFrame(frame);
			qs.mergeImplicitFilters(frame.getFrameFilters());
		}
		
		ITask task = new BasicIteratorTask(qs);
		// add the task to the store
		this.insight.getTaskStore().addTask(task);
		return task;
	}
}
