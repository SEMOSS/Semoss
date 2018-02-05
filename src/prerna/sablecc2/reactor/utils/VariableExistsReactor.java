package prerna.sablecc2.reactor.utils;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class VariableExistsReactor extends AbstractReactor {
	
	public VariableExistsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.VARIABLE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		boolean varExists = false;
		String variable = this.curRow.get(0).toString();
		// see if it is in the planner
		if(this.planner.hasVariable(variable)) {
			varExists = true;
		}
		return new NounMetadata(varExists, PixelDataType.BOOLEAN);
	}

}
