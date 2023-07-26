package prerna.sablecc2.reactor.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.io.connector.antivirus.VirusScannerUtils;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.security.AbstractHttpHelper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.git.reactors.CommitAssetReactor;

public class GetRequestReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetRequestReactor.class);

	public GetRequestReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.URL.getKey(), "headersMap", "useApplicationCert", "saveFile"};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String url = this.keyValue.get(this.keysToGet[0]);
		Utility.checkIfValidDomain(url);
		List<Map<String, String>> headersMap = getHeadersMap();
		String keyStore = null;
		String keyStorePass = null;
		boolean useApplicationCert = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[2]) + "");
		if(useApplicationCert) {
			keyStore = DIHelper.getInstance().getProperty(Constants.SCHEDULER_KEYSTORE);
			keyStorePass = DIHelper.getInstance().getProperty(Constants.SCHEDULER_KEYSTORE_PASSWORD);
		}
		
		boolean saveFile = false;
		if (this.keysToGet[3] != null) {	
			saveFile = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[3]) + "");
		}
		
		if (saveFile) {
			return file(headersMap, url, keyStore, keyStorePass);	
		} else {
			return nonFile(headersMap, url, keyStore, keyStorePass);	
		}	
	}
	
	private NounMetadata nonFile(List<Map<String, String>> headersMap, String url, String keyStore, String keyStorePass) {
		ResponseHandler<String> handler = new BasicResponseHandler();
		CloseableHttpResponse response = null;
		CloseableHttpClient httpClient = null;
		try {
			httpClient = AbstractHttpHelper.getCustomClient(null, keyStore, keyStorePass);
			HttpGet httpGet = new HttpGet(url);
			if(headersMap != null && !headersMap.isEmpty()) {
				for(int i = 0; i < headersMap.size(); i++) {
					Map<String, String> head = headersMap.get(i);
					for(String key : head.keySet()) {
						httpGet.addHeader(key, head.get(key));
					}
				}
			}
			response = httpClient.execute(httpGet);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Could not connect to URL at " + url);
		}
		
		String retString = null;
		try {
			retString = handler.handleResponse(response);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Could not connect to URL at " + url);
		} finally {
			if(response != null) {
				try {
					response.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			if(httpClient != null) {
				try {
					httpClient.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return new NounMetadata(retString, PixelDataType.CONST_STRING);
	}
	
	private NounMetadata file(List<Map<String, String>> headersMap, String url, String keyStore, String keyStorePass) {
		String[] pathSeparated = url.split("/");
		String filename = pathSeparated[pathSeparated.length - 1];
		
		if (filename == null) {
			throw new IllegalArgumentException("Url path does not end in a file name");
		}
		
		CloseableHttpResponse response = null;
		CloseableHttpClient httpClient = null;
		try {
			httpClient = AbstractHttpHelper.getCustomClient(null, keyStore, keyStorePass);
			HttpGet httpGet = new HttpGet(url);
			if(headersMap != null && !headersMap.isEmpty()) {
				for(int i = 0; i < headersMap.size(); i++) {
					Map<String, String> head = headersMap.get(i);
					for(String key : head.keySet()) {
						httpGet.addHeader(key, head.get(key));
					}
				}
			}
			response = httpClient.execute(httpGet);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Could not connect to URL at " + url);
		}
			
		String filePath = this.insight.getInsightFolder();
		File fileDir = new File(filePath);
		if (!fileDir.exists()) {
			Boolean success = fileDir.mkdirs();
			if(!success) {
				classLogger.info("Unable to make direction at location: " + Utility.cleanLogString(filePath));
			}
		}
		
		String fileLocation = getUniquePath(filePath, filename);
		File file = new File(fileLocation);
		
		InputStream is = null;
		// used if virus scanning
		ByteArrayOutputStream baos = null;
		ByteArrayInputStream bais = null;
		try {
			HttpEntity entity = response.getEntity(); 
			is = entity.getContent();
			
			if (Utility.isVirusScanningEnabled()) {
				try {
					baos = new ByteArrayOutputStream();
		            IOUtils.copy(is, baos);
		            bais = new ByteArrayInputStream(baos.toByteArray());
		            
					Map<String, Collection<String>> viruses = VirusScannerUtils.getViruses(filename, bais);
					if (!viruses.isEmpty()) {	
						String error = "File contained " + viruses.size() + " virus";
						if (viruses.size() > 1) {
							error = error + "es";
						}
						
						throw new IllegalArgumentException(error);
					}
					
					bais.reset();
					FileUtils.copyInputStreamToFile(bais, file);
				} catch (IOException e) {
					throw new IllegalArgumentException("Could not read file item.", e);
				}
			} else {
				FileUtils.copyInputStreamToFile(is, file);
			}
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			String message = "Could not connect to URL at " + url;
			if(e.getMessage() != null && !e.getMessage().isEmpty()) {
				message += ". Detailed message = " + e.getMessage();
			}
			throw new IllegalArgumentException(message);
		} finally {
			if(is != null) {
				IOUtils.closeQuietly(is);
			}
			if(bais != null) {
				IOUtils.closeQuietly(bais);
			}
			if(baos != null) {
				IOUtils.closeQuietly(baos);
			}
			if(response != null) {
				try {
					response.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			if(httpClient != null) {
				try {
					httpClient.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		String savedName = FilenameUtils.getName(fileLocation);
		classLogger.info("Saved Filename: " + savedName + " to "+ file);
		this.runCommitAssetReactor(fileLocation, savedName);
		
		return new NounMetadata(savedName, PixelDataType.CONST_STRING);
	}
	
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

	private String getUniquePath(String directory, String fileLocation) {
		String fileName = Utility.normalizePath(FilenameUtils.getBaseName(fileLocation).trim());
		String fileExtension = FilenameUtils.getExtension(fileLocation).trim();
		
		// h2 is weird and will not work if it doesn't end in .mv.db
		boolean isH2 = fileLocation.endsWith(".mv.db");
		File f = new File(directory + DIR_SEPARATOR + fileName + "." + fileExtension);
		int counter = 2;
		while(f.exists()) {
			if(isH2) {
				f = new File(directory + DIR_SEPARATOR + fileName.replace(".mv", "") + " (" + counter + ")" + ".mv.db");
			} else {
				f = new File(directory + DIR_SEPARATOR + fileName + " (" + counter + ")" + "." + fileExtension);
			}
			counter++;
		}
		
		return f.getAbsolutePath();
	}


	/**
	 * Get headers to add to the request
	 * @return
	 */
	private List<Map<String, String>> getHeadersMap() {
		GenRowStruct headersGrs = this.store.getNoun(this.keysToGet[1]);
		if(headersGrs != null && !headersGrs.isEmpty()) {
			List<Map<String, String>> headers = new Vector<Map<String, String>>();
			for(int i = 0; i < headersGrs.size(); i++) {
				headers.add( (Map<String, String>) headersGrs.get(i)); 
			}
			return headers;
		}
		return null;
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals("headersMap")) {
			return "Map containing key-value pairs to send in the GET request";
		} else if(key.equals("useApplicationCert")) {
			return "Boolean if we should use the default application certificate when making the request";
		} else if(key.equals("saveFile")) {
			return "Boolean if the request is returning a file and we should save the file to the insight space";
		}
		return super.getDescriptionForKey(key);
	}

}
