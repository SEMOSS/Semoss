package prerna.sablecc2.reactor.frame;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.ds.h2.H2Frame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.ds.r.RDataTable;

public class FrameFactory {

	public static ITableDataFrame getFrame(String frameType, String alias) {
		switch (frameType.toUpperCase()) {
		case "GRID": { return new H2Frame(alias); }
		case "GRAPH": { return new TinkerFrame(); } 
		case "RFRAME": { return new RDataTable(alias); }
		case "R": { return new RDataTable(alias); }
		case "NATIVE": { return new NativeFrame(); }
		default: { return new H2Frame(alias); }
		}
	}
}
