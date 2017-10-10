package prerna.sablecc2.reactor.panel.external;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;

public class OpenTabReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		String url = this.curRow.get(0).toString();
		return new NounMetadata(url, PixelDataType.CONST_STRING, PixelOperationType.OPEN_TAB);
	}

}
