package prerna.sablecc2.reactor.frame.rdbms;

import prerna.ds.h2.H2Frame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;

public class SortColumnReactor extends AbstractFrameReactor {

	@Override
	public NounMetadata execute() {
		H2Frame frame = (H2Frame) getFrame();
		GenRowStruct inputsGRS = this.getCurRow();
		String sortColumn = "";
		String sortOrder = "";
		
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			sortColumn = inputsGRS.getNoun(0).getValue() + "";
			sortOrder = inputsGRS.getNoun(1).getValue() + "";
		}
		
		return null;
	}

}
