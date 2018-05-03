package prerna.sablecc2.reactor.qs;

import prerna.algorithm.api.ITableDataFrame;
import prerna.query.querystruct.SelectQueryStruct;

public class QueryAllReactor extends AbstractQueryStructReactor {
	
	@Override
	protected SelectQueryStruct createQueryStruct() {
		ITableDataFrame frame = this.qs.getFrame();
		if(frame == null) {
			frame = (ITableDataFrame) this.insight.getDataMaker();
			this.qs.setFrame(frame);
		}
		if(frame != null) {
			this.qs.merge(frame.getMetaData().getFlatTableQs());
			this.qs.setImplicitFilters(frame.getFrameFilters());
		}
		return this.qs;
	}
	
}
