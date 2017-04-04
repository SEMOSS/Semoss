package prerna.sablecc2.reactor;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.ds.h2.H2Frame;

public class FrameFactory {

	public static ITableDataFrame getFrame(String frameType) {
		switch (frameType.toUpperCase()) {

		case "GRID": {
			return new H2Frame();
		}

		case "GRAPH": {
			return new TinkerFrame();
		}

		//		case "SPARK": {
		//			return new H2Frame();
		//		}
		//
		//		case "NATIVE": {
		//			return new H2Frame();
		//		}
		//		case "RFRAME": {
		//			return new H2Frame();
		//		}
		//		case "MSSQLSERVER": {
		//			return new H2Frame();
		//		}

		default: {
			return new H2Frame();
		}
		}

	}
}
