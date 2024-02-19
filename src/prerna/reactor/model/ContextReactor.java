package prerna.reactor.model;

import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.modelinference.ModelInferenceQueryStruct;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.util.Utility;

public class ContextReactor extends AbstractQueryStructReactor {

	@Override
	protected AbstractQueryStruct createQueryStruct()  {
		
		String context = null;
		if(!this.curRow.isEmpty()) {
			context = (String) this.curRow.get(0);
			if (context != null) {
				context = Utility.decodeURIComponent(context);
			}
		}
		
		((ModelInferenceQueryStruct) this.qs).setContext(context);
		return qs;
	}
}
