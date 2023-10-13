package prerna.reactor.frame;

import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.reactor.imports.ImportUtility;
import prerna.reactor.task.TaskBuilderReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;

public class CollectNewTemporalColReactor extends TaskBuilderReactor {

	public CollectNewTemporalColReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.QUERY_STRUCT.getKey() };
	}
	
	public NounMetadata execute() {
		if(! ((this.task=getTask()) instanceof BasicIteratorTask)) {
			throw new IllegalArgumentException("Can only add a new column using a basic query on a frame");
		}
		
		// get the query struct
		SelectQueryStruct sqs = ((BasicIteratorTask) this.task).getQueryStruct();
		ITableDataFrame frame = sqs.getFrame();
		
		OwlTemporalEngineMeta metadata = frame.getMetaData();
		SelectQueryStruct pqs = null;

		try {
			// convert to to the physical structure
			pqs = QSAliasToPhysicalConverter.getPhysicalQs(sqs, metadata);
		} catch(Exception ex) {
			return getWarning("Calculation is using columns that do not exist in the frame. Cannot perform this operation");
		}

		if(pqs.getCombinedFilters().getFilters() != null && pqs.getCombinedFilters().getFilters().size() > 0 ) {
			pqs.ignoreFilters = true;
		}

		// there should be only one selector
		List <IQuerySelector> allSelectors = sqs.getSelectors();
		if(allSelectors.size() == 0) {
			throw new IllegalArgumentException("No new columns to add");
		}
		
		// merge the results inside
		ImportUtility.parseQueryStructToFlatTable(frame, pqs, frame.getName(), this.task, true);

		NounMetadata noun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE);
		noun.addAdditionalReturn(getSuccess("Added Col " + allSelectors.get(0).getAlias()));
		return noun;
	}

	@Override
	public List<NounMetadata> getOutputs() {
		List<NounMetadata> outputs = super.getOutputs();
		if(outputs != null && !outputs.isEmpty()) return outputs;

		outputs = new Vector<NounMetadata>();
		NounMetadata output = new NounMetadata(this.signature, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
		outputs.add(output);
		return outputs;
	}

	@Override
	protected void buildTask() {
		// do nothing
		
	}
}
