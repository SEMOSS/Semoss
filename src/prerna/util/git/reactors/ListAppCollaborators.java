package prerna.util.git.reactors;

import java.util.List;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.git.GitCollaboratorUtils;

public class ListAppCollaborators extends AbstractReactor {

	public ListAppCollaborators() {
		this.keysToGet = new String[]{ReactorKeysEnum.REPOSITORY.getKey(), 
				ReactorKeysEnum.USERNAME.getKey(), ReactorKeysEnum.PASSWORD.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		Logger logger = getLogger(this.getClass().getName());
		logger.info("Establishing connection ");
		logger.info("This can take several minutes depending on the speed of your internet ");
		
		String repository = this.keyValue.get(this.keysToGet[0]);
		String username = this.keyValue.get(this.keysToGet[1]);
		String password = this.keyValue.get(this.keysToGet[2]);
		
		logger.info("Listing Collaborators");
		List<String> collabs = GitCollaboratorUtils.listCollaborators(repository, username, password);
		return new NounMetadata(collabs, PixelDataType.VECTOR, PixelOperationType.MARKET_PLACE);
	}

}
