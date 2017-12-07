package prerna.sablecc2.reactor.git;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.GitHelper;

public class LoginGit extends AbstractReactor {

	public LoginGit()
	{
		this.keysToGet = new String[]{"username", "password"};
	}
	
	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		Logger logger = getLogger(this.getClass().getName());
		try {
			organizeKeys();
			
			logger.info("Logging In");
			
			boolean valid = new GitHelper().login(keyValue.get(keysToGet[0]), keyValue.get(keysToGet[1]));
			// if nothing is sent it means it is dual
			logger.info("Complete");

			return new NounMetadata(valid, PixelDataType.BOOLEAN, PixelOperationType.MARKET_PLACE);
		} catch (Exception e) {
			e.printStackTrace();
			logger.fatal(e.getMessage());
		}
		return null;
	}
	
	/**
	 * Need to get the appropriate files to perform indexing!
	 * @param filesChanged
	 * @return
	 */
	private Map<String, List<String>> getMosfetFiles(Map <String, List<String>> filesChanged) {
		Map<String, List<String>> mosfetFiles = new Hashtable<String, List<String>>();
		if(filesChanged.containsKey("ADD")) {
			List<String> files = getMosfetFiles(filesChanged.get("ADD"));
			if(!files.isEmpty()) {
				mosfetFiles.put("ADD", files);
			}
		}
		
		if(filesChanged.containsKey("MOD")) {
			List<String> files = getMosfetFiles(filesChanged.get("ADD"));
			if(!files.isEmpty()) {
				mosfetFiles.put("ADD", files);
			}		
		}
		
		if(filesChanged.containsKey("REN")) {
			List<String> files = getMosfetFiles(filesChanged.get("ADD"));
			if(!files.isEmpty()) {
				mosfetFiles.put("ADD", files);
			}		
		}
		
		if(filesChanged.containsKey("DEL")) {
			List<String> files = getMosfetFiles(filesChanged.get("ADD"));
			if(!files.isEmpty()) {
				mosfetFiles.put("ADD", files);
			}		
		}
		return mosfetFiles;
	}
	
	private List<String> getMosfetFiles(List<String> potentialFiles) {
		List<String> mosfetFiles = new Vector<String>();
		if(potentialFiles != null) {
			for(String f : potentialFiles) {
				if(f.endsWith(".mosfet")) {
					mosfetFiles.add(f);
				}
			}
		}
		return mosfetFiles;
	}

}
