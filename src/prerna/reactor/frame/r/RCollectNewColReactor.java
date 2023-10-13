package prerna.reactor.frame.r;

import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.ICodeExecution;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.om.Variable.LANGUAGE;
import prerna.query.interpreters.RInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.reactor.task.TaskBuilderReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;

public class RCollectNewColReactor extends TaskBuilderReactor implements ICodeExecution {

	/**
	 * This class is responsible for collecting data from a task and returning it
	 */

	private String codeExecuted = null;
	private AbstractRJavaTranslator rJavaTranslator = null;
	
	public RCollectNewColReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.QUERY_STRUCT.getKey() };
	}

	public NounMetadata execute() {
		String warning = null;

		if(! ((this.task=getTask()) instanceof BasicIteratorTask)) {
			throw new IllegalArgumentException("Can only add a new column using a basic query on a frame");
		}
		
		// get the query struct
		SelectQueryStruct sqs = ((BasicIteratorTask) this.task).getQueryStruct();
		RDataTable frame = (RDataTable) sqs.getFrame();
		rJavaTranslator = this.insight.getRJavaTranslator(this.getLogger(this.getClass().getName()));
		rJavaTranslator.startR(); 
		
		OwlTemporalEngineMeta metadata = frame.getMetaData();
		SelectQueryStruct pqs = null;
		
		try {
			// if the columns are not there. 
			pqs = QSAliasToPhysicalConverter.getPhysicalQs(sqs, metadata);
		} catch(Exception ex) {
			return getWarning("Calculation is using columns that do not exist in the frame. Cannot perform this operation");
		}
		
		if(pqs.getCombinedFilters().getFilters() != null && pqs.getCombinedFilters().getFilters().size() > 0 )
		{
			warning = "You are applying a calculation while there are filters, this is not recommended and can lead to unpredictable results";
			pqs.ignoreFilters = true;
		}
		
		RInterpreter interp = new RInterpreter();
		interp.setQueryStruct(pqs);
		interp.setDataTableName(frame.getName());
		interp.setColDataTypes(metadata.getHeaderToTypeMap());
		interp.setLogger(this.getLogger(this.getClass().getName()));
		String query = interp.composeQuery();
		
		sqs.ignoreFilters = false;
		// query comes to this
		//tempaqBZQYvZoVE <- unique(mv[ , { V0=(RevenueInternational * 2) ; list(new_col=V0) }]);

		// need to drop all the way to unique
		// and then add it as a var on the frame
	
		String mainQuery = query.split("<-")[1];
		mainQuery = mainQuery.replaceFirst("unique", "");
		
		// need to get the query struct
		// there should be only one selector
		List <IQuerySelector> allSelectors = sqs.getSelectors();
		if(allSelectors.size() == 0) {
			throw new IllegalArgumentException("No new columns to add");
		}
		
		IQuerySelector onlySelector = allSelectors.get(0);
		
		String alias = onlySelector.getAlias();
		mainQuery = frame.getName() + "$" + alias + "  <- " + mainQuery;
		rJavaTranslator.executeEmptyR(mainQuery);
		this.codeExecuted = mainQuery;

		// recreate the frame metadata
		frame.recreateMeta();

		NounMetadata noun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE);
		noun.addAdditionalReturn(getSuccess("Added Col " + alias));
		if(warning != null) {
			noun.addAdditionalReturn(getWarning(warning));
		}

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

	@Override
	public String getExecutedCode() {
		return this.codeExecuted;
	}

	@Override
	public LANGUAGE getLanguage() {
		return LANGUAGE.R;
	}

	@Override
	public boolean isUserScript() {
		return false;
	}
}
