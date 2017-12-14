package prerna.sablecc2.reactor.git;

import java.util.Hashtable;
import java.util.List;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.GitHelper;

public class GitStatusReactor extends AbstractReactor {

	public GitStatusReactor() {
		this.keysToGet = new String[]{"app"};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		GitHelper helper = new GitHelper();
		Hashtable<String, List<String>> fileInfo = helper.getStatus(this.keyValue.get(this.keysToGet[0]));
		return new NounMetadata(fileInfo, PixelDataType.MAP, PixelOperationType.MARKET_PLACE);
	}

}
