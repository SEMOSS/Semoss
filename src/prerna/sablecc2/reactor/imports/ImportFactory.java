package prerna.sablecc2.reactor.imports;

import java.util.Iterator;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.ds.h2.H2Frame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.ds.py.PandasFrame;
import prerna.ds.r.RDataTable;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.QueryStruct2;

public class ImportFactory {

	public static IImporter getImporter(ITableDataFrame frame, QueryStruct2 qs) {
		if(frame instanceof H2Frame) {
			return new H2Importer((H2Frame) frame, qs);
		} else if(frame instanceof TinkerFrame) {
			return new TinkerImporter((TinkerFrame) frame, qs);
		} else if(frame instanceof RDataTable) {
			return new RImporter((RDataTable) frame, qs);
		} else if(frame instanceof PandasFrame) {
			return new PandasImporter((PandasFrame) frame, qs);
		} else if(frame instanceof NativeFrame) {
			return new NativeFrameImporter((NativeFrame) frame, qs);
		}
		return null;
	}
	
	public static IImporter getImporter(ITableDataFrame frame, QueryStruct2 qs, Iterator<IHeadersDataRow> it) {
		if(frame instanceof H2Frame) {
			return new H2Importer((H2Frame) frame, qs, it);
		} else if(frame instanceof TinkerFrame) {
			return new TinkerImporter((TinkerFrame) frame, qs, it);
		} else if(frame instanceof RDataTable) {
			return new RImporter((RDataTable) frame, qs, it);
		} else if(frame instanceof PandasFrame) {
			return new PandasImporter((PandasFrame) frame, qs, it);
		} 
		return null;
	}
}
