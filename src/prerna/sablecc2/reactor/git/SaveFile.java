package prerna.sablecc2.reactor.git;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.DIHelper;
import prerna.util.GitHelper;

public class SaveFile extends AbstractReactor {

	// clone from remote
	// this assumes the local directory is not there
	
	public SaveFile()
	{
		// file patterns are usually separated by ;
		// I am hoping this is either a * or a single file
		
		super.keysToGet = new String[]{"app", "file"};
	}
	
	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		// creates a remote repository
		organizeKeys();
		Logger logger = getLogger(this.getClass().getName());
		
		logger.info("Versioning File : " + keyValue.get(keysToGet[0]));
		GitHelper helper = new GitHelper();

		logger.info("Initialized");
		
		String [] files = keyValue.get(keysToGet[1]).split(";");
		
		

		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		
		// add the files first
		helper.addFiles(baseFolder + "/db/" + keyValue.get(keysToGet[0]), files);

		// commit it now
		helper.commit(baseFolder + "/db/" + keyValue.get(keysToGet[0]));
		
		return new NounMetadata("Success", PixelDataType.CONST_STRING, PixelOperationType.MARKET_PLACE);
	}

}
