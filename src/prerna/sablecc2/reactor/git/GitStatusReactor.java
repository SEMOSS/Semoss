package prerna.sablecc2.reactor.git;

import java.util.List;
import java.util.Map;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.git.GitUtils;

public class GitStatusReactor extends AbstractReactor {

	public GitStatusReactor() {
		this.keysToGet = new String[]{"app"};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		List<Map<String, String>> fileInfo = GitUtils.getStatus(this.keyValue.get(this.keysToGet[0]));
		return new NounMetadata(fileInfo, PixelDataType.VECTOR, PixelOperationType.MARKET_PLACE);
	}

}
