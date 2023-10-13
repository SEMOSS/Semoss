package prerna.reactor.imports;

import java.util.Iterator;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.ds.py.PandasFrame;
import prerna.ds.r.RDataTable;
import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.SelectQueryStruct;

public class ImportFactory {

	public static IImporter getImporter(ITableDataFrame frame, SelectQueryStruct qs) {
		if(frame instanceof AbstractRdbmsFrame) {
			return new RdbmsImporter((AbstractRdbmsFrame) frame, qs);
		} else if(frame instanceof TinkerFrame) {
			return new TinkerImporter((TinkerFrame) frame, qs);
		} else if(frame instanceof RDataTable) {
			return new RImporter((RDataTable) frame, qs);
		} else if(frame instanceof PandasFrame) {
			return new PandasImporter((PandasFrame) frame, qs);
		} else if(frame instanceof NativeFrame) {
			return new NativeImporter((NativeFrame) frame, qs);
		}
		return null;
	}
	
	public static IImporter getImporter(ITableDataFrame frame, SelectQueryStruct qs, Iterator<IHeadersDataRow> it) {
		if(frame instanceof AbstractRdbmsFrame) {
			return new RdbmsImporter((AbstractRdbmsFrame) frame, qs, it);
		} else if(frame instanceof TinkerFrame) {
			return new TinkerImporter((TinkerFrame) frame, qs, it);
		} else if(frame instanceof RDataTable) {
			return new RImporter((RDataTable) frame, qs, it);
		} else if(frame instanceof PandasFrame) {
			return new PandasImporter((PandasFrame) frame, qs, it);
		} else if(frame instanceof NativeFrame) {
			return new NativeImporter((NativeFrame) frame, qs, it);
		}
		return null;
	}
}
