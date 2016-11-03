package prerna.sablecc;

import java.util.Iterator;

import prerna.ds.TinkerFrame;
import prerna.ds.h2.H2Frame;
import prerna.ds.spark.SparkDataFrame;

public class DataTypeReactor extends AbstractReactor {

	@Override
	public Iterator process() {
		// get the frame
		if (myStore.get("G") instanceof H2Frame) {
			myStore.put("data.type", "H2Frame");
		} else if (myStore.get("G") instanceof TinkerFrame) {
			myStore.put("data.type", "TinkerFrame");
		} else if (myStore.get("G") instanceof SparkDataFrame) {
			myStore.put("data.type", "SparkDataFrame");
		}

		return null;
	}

}
