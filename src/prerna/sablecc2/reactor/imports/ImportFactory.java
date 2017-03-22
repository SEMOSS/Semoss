package prerna.sablecc2.reactor.imports;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.ds.h2.H2Frame;

public class ImportFactory {

	public static Importer getImporter(ITableDataFrame frame) {
		if(frame instanceof H2Frame) {
			return new H2Importer();
		} else if(frame instanceof TinkerFrame) {
			return new TinkerImporter();
		}
		return null;
	}
}
