package prerna.reactor.qs.selectors;

import java.util.List;

import prerna.query.querystruct.SelectQueryStruct;
import prerna.reactor.AbstractReactor;
import prerna.reactor.qs.SubQueryExpression;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SubQueryExpressionReactor extends AbstractReactor {	
	
	public SubQueryExpressionReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.QUERY_STRUCT.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		// grab the input qs
		SelectQueryStruct qs = null;
		GenRowStruct qsInputParams = this.store.getNoun(PixelDataType.QUERY_STRUCT.getKey());
		if(qsInputParams != null && !qsInputParams.isEmpty()) {
			qs = (SelectQueryStruct) qsInputParams.get(0);
		}
		if(qs == null) {
			List<NounMetadata> qsInputs = this.curRow.getNounsOfType(PixelDataType.QUERY_STRUCT);
			if(qsInputs != null && !qsInputs.isEmpty()) {
				qs = (SelectQueryStruct) qsInputs.get(0).getValue();
			}
		}
		
		if(qs == null) {
			throw new NullPointerException("Must pass a QS for the SubQueryExpression");
		}
		
		SubQueryExpression expression = new SubQueryExpression();
		expression.setQs(qs);
		expression.setInsight(this.insight);
		return new NounMetadata(expression, PixelDataType.SUB_QUERY_EXPRESSION);
	}
	
}
