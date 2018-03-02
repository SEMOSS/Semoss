package prerna.sablecc2.reactor.qs;

import prerna.algorithm.api.ITableDataFrame;
import prerna.query.querystruct.QueryStruct2;

public class QueryAllReactor extends AbstractQueryStructReactor {
	
	@Override
	protected QueryStruct2 createQueryStruct() {
		ITableDataFrame frame = this.qs.getFrame();
		if(frame == null) {
			frame = (ITableDataFrame) this.insight.getDataMaker();
			this.qs.setFrame(frame);
		}
		
		this.qs.merge(frame.getMetaData().getFlatTableQs());
		this.qs.setImplicitFilters(frame.getFrameFilters());
		return this.qs;
	}
	
}
