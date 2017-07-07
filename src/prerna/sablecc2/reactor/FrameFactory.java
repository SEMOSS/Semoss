package prerna.sablecc2.reactor;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.ds.h2.H2Frame;
import prerna.ds.r.RDataTable;

public class FrameFactory {

	public static ITableDataFrame getFrame(String frameType) {
		switch (frameType.toUpperCase()) {
		case "GRID": { return new H2Frame(); }
		case "GRAPH": { return new TinkerFrame(); } 
		case "RFRAME": { return new RDataTable(); }
		case "R": { return new RDataTable(); }
		default: { return new H2Frame(); }
		}
	}
}
