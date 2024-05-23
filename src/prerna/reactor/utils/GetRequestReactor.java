package prerna.reactor.utils;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.project.impl.ProjectHeaderAuthEvaluator;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.security.AbstractHttpHelper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.git.reactors.CommitAssetReactor;

public class GetRequestReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetRequestReactor.class);

	public GetRequestReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.URL.getKey(), ReactorKeysEnum.HEADERS_MAP.getKey(),
				ReactorKeysEnum.USE_APPLICATION_CERT.getKey(), "saveFile"};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String url = this.keyValue.get(this.keysToGet[0]);
		Utility.checkIfValidDomain(url);
		Map<String, String> headersMap = getHeadersMap();
		String keyStore = null;
		String keyStorePass = null;
		String keyPass = null;
		boolean useApplicationCert = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[2]) + "");
		if(useApplicationCert) {
			keyStore = DIHelper.getInstance().getProperty(Constants.SCHEDULER_KEYSTORE);
			keyStorePass = DIHelper.getInstance().getProperty(Constants.SCHEDULER_KEYSTORE_PASSWORD);
			keyPass = DIHelper.getInstance().getProperty(Constants.SCHEDULER_CERTIFICATE_PASSWORD);
		}
		
		boolean saveFile = false;
		if (this.keysToGet[3] != null) {	
			saveFile = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[3]) + "");
		}
		
		if (saveFile) {
			return file(url, headersMap, keyStore, keyStorePass, keyPass);	
		} else {
			return nonFile(url, headersMap, keyStore, keyStorePass, keyPass);	
		}	
	}
	
	/**
	 * 
	 * @param headersMap
	 * @param url
	 * @param keyStore
	 * @param keyStorePass
	 * @param keyPass
	 * @return
	 */
	private NounMetadata nonFile(String url, Map<String, String> headersMap, String keyStore, String keyStorePass, String keyPass) {
		return new NounMetadata(AbstractHttpHelper.getRequest(url, headersMap, keyStore, keyStorePass, keyPass), PixelDataType.CONST_STRING);
	}
	
	/**
	 * 
	 * @param headersMap
	 * @param url
	 * @param keyStore
	 * @param keyStorePass
	 * @param keyPass
	 * @return
	 */
	private NounMetadata file(String url, Map<String, String> headersMap, String keyStore, String keyStorePass, String keyPass) {
		String filePath = this.insight.getInsightFolder();
		File savedFile = AbstractHttpHelper.getRequestFileDownload(url, headersMap, keyStore, keyStorePass, keyPass, filePath, null);
		String savedFilePath = savedFile.getAbsolutePath();
		String savedFileName = FilenameUtils.getName(savedFilePath);
		// we only commit if its a saved insight
		if(this.insight.isSavedInsight()) {
			this.runCommitAssetReactor(savedFilePath, savedFileName);
		}
		return new NounMetadata(savedFileName, PixelDataType.CONST_STRING);
	}
	
	/**
	 * 
	 * @param fileLocation
	 * @param savedName
	 */
	private void runCommitAssetReactor(String fileLocation, String savedName) {
		CommitAssetReactor car = new CommitAssetReactor();
		car.In();
		
		GenRowStruct grs1 = new GenRowStruct();
		grs1.add(new NounMetadata(fileLocation, PixelDataType.CONST_STRING));
		car.getNounStore().addNoun(ReactorKeysEnum.FILE_PATH.getKey(), grs1);
		
		GenRowStruct grs2 = new GenRowStruct();
		String comment = "GetRequestReactor ran for file " + savedName;
		grs2.add(new NounMetadata(comment, PixelDataType.CONST_STRING));
		car.getNounStore().addNoun(ReactorKeysEnum.COMMENT_KEY.getKey(), grs2);
		
		car.setInsight(this.insight);
		car.execute();
	}

	/**
	 * Get headers to add to the request
	 * @return
	 */
	private Map<String, String> getHeadersMap() {
		GenRowStruct headersGrs = this.store.getNoun(this.keysToGet[1]);
		if(headersGrs != null && !headersGrs.isEmpty()) {
			Map<String, String> headers = new HashMap<>();
			for(int i = 0; i < headersGrs.size(); i++) {
				NounMetadata noun = headersGrs.getNoun(i);
				if(noun.getNounType() == PixelDataType.PROJECT_AUTHORIZATION_HEADER) {
					try {
						headers.putAll( ((ProjectHeaderAuthEvaluator) noun.getValue()).eval() );
					} catch (UnsupportedEncodingException e) {
						classLogger.error(Constants.STACKTRACE, e);
						throw new IllegalArgumentException("An error occurred trying to get the project authorization headers");
					}
				} else {
					headers.putAll( (Map<String, String>) noun.getValue() );
				}
			}
			return headers;
		}
		return null;
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals("headersMap")) {
			return "Map containing key-value pairs to send in the GET request";
		} else if(key.equals("saveFile")) {
			return "Boolean if the request is returning a file and we should save the file to the insight space";
		}
		return super.getDescriptionForKey(key);
	}

}
