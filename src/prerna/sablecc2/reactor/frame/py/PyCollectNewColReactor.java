package prerna.sablecc2.reactor.frame.py;

import java.util.List;
import java.util.Vector;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.py.PandasFrame;
import prerna.ds.py.PyTranslator;
import prerna.query.interpreters.PandasInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.reactor.task.TaskBuilderReactor;

public class PyCollectNewColReactor extends TaskBuilderReactor {

	/**
	 * This class is responsible for collecting data from a task and returning it
	 */

	PyTranslator pyt = null;

	public PyCollectNewColReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.QUERY_STRUCT.getKey() };
	}

	public NounMetadata execute() {
		String warning = null;
		
		if(! ((this.task=getTask()) instanceof BasicIteratorTask)) {
			throw new IllegalArgumentException("Can only add a new column using a basic query on a frame");
		}
		
		// get the query struct
		SelectQueryStruct sqs = ((BasicIteratorTask) this.task).getQueryStruct();
		PandasFrame frame = (PandasFrame) sqs.getFrame();
		pyt = insight.getPyTranslator();
		
		OwlTemporalEngineMeta metadata = frame.getMetaData();
		SelectQueryStruct pqs = null;

		try {
			// if the columns are not there. 
			pqs = QSAliasToPhysicalConverter.getPhysicalQs(sqs, metadata);
		} catch(Exception ex) {
			return getWarning("Calculation is using columns that do not exist in the frame. Cannot perform this operation");
		}

		if(pqs.getCombinedFilters().getFilters() != null && pqs.getCombinedFilters().getFilters().size() > 0 ) {
			warning = "You are applying a calculation while there are filters, this is not recommended and can lead to unpredictable results";
			pqs.ignoreFilters = true;
		}

		List<NounMetadata> outputs = new Vector<NounMetadata>();
		// there should be only one selector
		List <IQuerySelector> allSelectors = sqs.getSelectors();
		if(allSelectors.size() > 0) {
			IQuerySelector onlySelector = allSelectors.get(0);
			PandasInterpreter interp = new PandasInterpreter();
			interp.setDataTableName(frame.getName(), frame.getName() + "w.cache['data']");
			interp.setDataTypeMap(frame.getMetaData().getHeaderToTypeMap());
			// I should also possibly set up pytranslator so I can run command for creating filter
			interp.setPyTranslator(pyt);
			String mainQuery = interp.processSelector(onlySelector, frame.getName(), false, false);

			//String mainQuery = processSelector(onlySelector, frame.getName()).toString();
			
			String alias = onlySelector.getAlias();
			mainQuery = frame.getName() + "['" + alias + "'] = " + mainQuery;
			pyt.runEmptyPy(mainQuery);

			// recreate the metadata
			frame.recreateMeta();
			
			outputs.add(new NounMetadata("Added Col " + alias, PixelDataType.CONST_STRING));
			outputs.add(new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE));
			if(warning != null) {
				outputs.add(getWarning(warning));
			}
		} else {
			outputs.add(new NounMetadata("No New Columns to add", PixelDataType.CONST_STRING));
		}

		return new NounMetadata(outputs, PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
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
