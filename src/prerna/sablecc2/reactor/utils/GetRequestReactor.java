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
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
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
		this.keysToGet = new String[]{ReactorKeysEnum.URL.getKey(), "headersMap", 
				ReactorKeysEnum.USE_APPLICATION_CERT.getKey(), "saveFile"};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String url = this.keyValue.get(this.keysToGet[0]);
		Utility.checkIfValidDomain(url);
		List<Map<String, String>> headersMap = getHeadersMap();
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
			return file(headersMap, url, keyStore, keyStorePass, keyPass);	
		} else {
			return nonFile(headersMap, url, keyStore, keyStorePass, keyPass);	
		}	
	}
	
	private NounMetadata nonFile(List<Map<String, String>> headersMap, String url, String keyStore, String keyStorePass, String keyPass) {
		CloseableHttpResponse response = null;
		CloseableHttpClient httpClient = null;
		HttpEntity entity = null;
		String responseData = null;
		try {
			httpClient = AbstractHttpHelper.getCustomClient(null, keyStore, keyStorePass, keyPass);
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
			int statusCode = response.getStatusLine().getStatusCode();
			entity = response.getEntity();
            if (statusCode >= 200 && statusCode < 300) {
                responseData = entity != null ? EntityUtils.toString(entity) : null;
            } else {
                responseData = entity != null ? EntityUtils.toString(entity) : "";
    			throw new IllegalArgumentException("Connected to " + url + " but received error = " + responseData);
            }
			
    		return new NounMetadata(responseData, PixelDataType.CONST_STRING);

		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Could not connect to URL at " + url);
		} finally {
			if(entity != null) {
				try {
					EntityUtils.consume(entity);
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
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
	}
	
	private NounMetadata file(List<Map<String, String>> headersMap, String url, String keyStore, String keyStorePass, String keyPass) {
		String[] pathSeparated = url.split("/");
		String filename = pathSeparated[pathSeparated.length - 1];
		
		if (filename == null) {
			throw new IllegalArgumentException("Url path does not end in a file name");
		}
		
		CloseableHttpResponse response = null;
		CloseableHttpClient httpClient = null;
		InputStream is = null;
		// used if virus scanning
		ByteArrayOutputStream baos = null;
		ByteArrayInputStream bais = null;
		HttpEntity entity = null;

		try {
			httpClient = AbstractHttpHelper.getCustomClient(null, keyStore, keyStorePass, keyPass);
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

			entity = response.getEntity(); 
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
			
			String savedName = FilenameUtils.getName(fileLocation);
			classLogger.info("Saved Filename: " + savedName + " to "+ file);
			this.runCommitAssetReactor(fileLocation, savedName);
			
			return new NounMetadata(savedName, PixelDataType.CONST_STRING);			
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Could not connect to URL at " + url);
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
			if(entity != null) {
				try {
					EntityUtils.consume(entity);
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
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
		} else if(key.equals("saveFile")) {
			return "Boolean if the request is returning a file and we should save the file to the insight space";
		}
		return super.getDescriptionForKey(key);
	}

}
