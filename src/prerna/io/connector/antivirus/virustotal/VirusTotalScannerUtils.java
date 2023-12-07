package prerna.io.connector.antivirus.virustotal;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.commons.io.FilenameUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.bettercloud.vault.json.Json;
import com.bettercloud.vault.json.JsonObject;
import com.bettercloud.vault.json.JsonValue;

import prerna.io.connector.antivirus.IVirusScanner;
import prerna.security.AbstractHttpHelper;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class VirusTotalScannerUtils implements IVirusScanner {

	private static final Logger classLogger = LogManager.getLogger(VirusTotalScannerUtils.class);

	public static final String VIRUSTOTAL_API_KEY = "VIRUSTOTAL_API_KEY";
	public static final String VIRUSTOTAL_USE_CERT = "VIRUSTOTAL_USE_CERT";

	private static VirusTotalScannerUtils instance;
	private String apiKey = null;
	private boolean useServerCert = false;
	
	private VirusTotalScannerUtils() throws Exception {
		this.apiKey = getApiKey();
		this.useServerCert = useServerCert();
	}

	public static IVirusScanner getInstance() {
		if(instance != null) {
			return instance;
		}

		if(instance == null) {
			synchronized(VirusTotalScannerUtils.class) {
				if(instance == null) {
					try {
						instance = new VirusTotalScannerUtils();
					} catch (Exception e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}

		return instance;
	}


	@Override
	public Map<String, Collection<String>> getViruses(String name, InputStream is) {
		String keyStore = null;
		String keyStorePass = null;
		String keyPass = null;
		if(this.useServerCert) {
			keyStore = DIHelper.getInstance().getProperty(Constants.SCHEDULER_KEYSTORE);
			keyStorePass = DIHelper.getInstance().getProperty(Constants.SCHEDULER_KEYSTORE_PASSWORD);
			keyPass = DIHelper.getInstance().getProperty(Constants.SCHEDULER_CERTIFICATE_PASSWORD);
		}
		
		String analysisFileId = uploadFileEndpoint(name, is, keyStore, keyStorePass, keyPass);
		return analysesEndpoint(analysisFileId, name, keyStore, keyStorePass, keyPass);
	}

	/**
	 * Upload the file and get back the analysis id value
	 * @param name
	 * @param is
	 * @return
	 */
	private String uploadFileEndpoint(String name, InputStream is, String keyStore, String keyStorePass, String keyPass) {
		final String VIRUS_TOTAL_URL = "https://www.virustotal.com/api/v3/files";
		
        String responseData = null;
        CloseableHttpClient httpClient = null;
		CloseableHttpResponse response = null;
		HttpEntity entity = null;
		try {
			httpClient = AbstractHttpHelper.getCustomClient(null, keyStore, keyStorePass, keyPass);
			HttpPost httpPost = new HttpPost(VIRUS_TOTAL_URL);
			httpPost.addHeader("x-apikey", this.apiKey);
			httpPost.addHeader("accept", "application/json");
			
			// attach the file
			MultipartEntityBuilder builder = MultipartEntityBuilder.create();
			builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
			builder.addBinaryBody(
			    "file",
			    is,
			    ContentType.create(FilenameUtils.getExtension(name)),
			    name
			);

			HttpEntity multipart = builder.build();
			httpPost.setEntity(multipart);
			
			response = httpClient.execute(httpPost);
			int statusCode = response.getStatusLine().getStatusCode();
			entity = response.getEntity();
            if (statusCode >= 200 && statusCode < 300) {
                responseData = entity != null ? EntityUtils.toString(entity) : null;
            } else {
                responseData = entity != null ? EntityUtils.toString(entity) : "";
    			throw new IllegalArgumentException("Connected to " + VIRUS_TOTAL_URL + " but received error = " + responseData);
            }
			
            /*
    		 * Example response:
    		   {
    			  "data": {
    			    "type": "analysis",
    			    "id": "ZmVjN2ZmM2MxN2RlZTE0NjUxNTg1ZjMwMDY0NjEzZDE6MTY5MDM3MzczOQ==",
    			    "links": {
    			      "self": "https://www.virustotal.com/api/v3/analyses/ZmVjN2ZmM2MxN2RlZTE0NjUxNTg1ZjMwMDY0NjEzZDE6MTY5MDM3MzczOQ=="
    			    }
    			  }
    			}
    		 * 
    		 */
    		JsonObject jsonResponse = Json.parse(responseData).asObject();
    		String analysisFileId = jsonResponse.get("data").asObject().getString("id");
    		return analysisFileId;
    		
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Could not connect to URL at " + VIRUS_TOTAL_URL);
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

	/**
	 * 
	 * @param analysisFileId
	 * @param keyStore
	 * @param keyStorePass
	 */
	private Map<String, Collection<String>> analysesEndpoint(String analysisFileId, String name, String keyStore, String keyStorePass, String keyPass) {
//		HttpRequest request = HttpRequest.newBuilder()
//				.uri(URI.create("https://www.virustotal.com/api/v3/analyses/ZmVjN2ZmM2MxN2RlZTE0NjUxNTg1ZjMwMDY0NjEzZDE6MTY5MDM3MzczOQ%3D%3D"))
//				.header("accept", "application/json")
//				.header("x-apikey", "21afaa2a137e9a132f14c052184b5c3c3fa8609eafc9bf315731dcd505244916")
//				.method("GET", HttpRequest.BodyPublishers.noBody())
//				.build();
//		HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
//		System.out.println(response.body());
		final String VIRUS_TOTAL_URL = "https://www.virustotal.com/api/v3/analyses/";

		
		String responseData = null;
		CloseableHttpClient httpClient = null;
		CloseableHttpResponse response = null;
		HttpEntity entity = null;
		try {
			httpClient = AbstractHttpHelper.getCustomClient(null, keyStore, keyStorePass, keyPass);
			HttpGet httpGet = new HttpGet(VIRUS_TOTAL_URL+analysisFileId);
			httpGet.addHeader("x-apikey", this.apiKey);
			httpGet.addHeader("accept", "application/json");
			
			response = httpClient.execute(httpGet);
			int statusCode = response.getStatusLine().getStatusCode();
			entity = response.getEntity();
            if (statusCode >= 200 && statusCode < 300) {
                responseData = entity != null ? EntityUtils.toString(entity) : null;
            } else {
                responseData = entity != null ? EntityUtils.toString(entity) : "";
    			throw new IllegalArgumentException("Connected to " + VIRUS_TOTAL_URL + " but received error = " + responseData);
            }
			
            JsonObject jsonResponse = Json.parse(responseData).asObject();
    		JsonObject dataAttributesJson = jsonResponse.get("data").asObject().get("attributes").asObject();
    		JsonObject overallStats = dataAttributesJson.get("stats").asObject();
    		if(overallStats.getInt("malicious") == 0 && overallStats.getInt("suspicious") == 0) {
    			return new HashMap<>();
    		}
    		
    		Map<String, Collection<String>> retMap = new HashMap<>();
    		Collection<String> allIssues = new TreeSet<>();
    		retMap.put(name, allIssues);
    		JsonObject resultsJson = dataAttributesJson.get("results").asObject();
    		List<String> categories = resultsJson.names();
    		for(String category : categories) {
    			JsonObject catObject = resultsJson.get(category).asObject();
    			JsonValue results = catObject.get("result");
    			if(results.isNull()) {
    				continue;
    			} else {
    				String issue = results.asString();
    				allIssues.add(issue);
    			}
    		}
    		
    		return retMap;
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Could not connect to URL at " + VIRUS_TOTAL_URL);
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
		
		/*
		 * 
		 * Example response:
		 * {
			  "meta": {
			    "file_info": {
			      "sha256": "3bf08e880ac1951de5980559e49e544546b918a7b60677c638e57a41b1927e11",
			      "sha1": "4d1da78a80c1cbadfaaa7209370571378811e804",
			      "md5": "fec7ff3c17dee14651585f30064613d1",
			      "size": 57108
			    }
			  },
			  "data": {
			    "attributes": {
			      "date": 1690373739,
			      "status": "completed",
			      "stats": {
			        "harmless": 0,
			        "type-unsupported": 16,
			        "suspicious": 0,
			        "confirmed-timeout": 0,
			        "timeout": 0,
			        "failure": 0,
			        "malicious": 0,
			        "undetected": 59
			      },
			      "results": {
			        "Bkav": {
			          "category": "undetected",
			          "engine_name": "Bkav",
			          "engine_version": "2.0.0.1",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "Lionic": {
			          "category": "undetected",
			          "engine_name": "Lionic",
			          "engine_version": "7.5",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "tehtris": {
			          "category": "type-unsupported",
			          "engine_name": "tehtris",
			          "engine_version": "v0.1.4",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "MicroWorld-eScan": {
			          "category": "undetected",
			          "engine_name": "MicroWorld-eScan",
			          "engine_version": "14.0.409.0",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "ClamAV": {
			          "category": "undetected",
			          "engine_name": "ClamAV",
			          "engine_version": "1.1.0.0",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "CMC": {
			          "category": "undetected",
			          "engine_name": "CMC",
			          "engine_version": "2.4.2022.1",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230619"
			        },
			        "CAT-QuickHeal": {
			          "category": "undetected",
			          "engine_name": "CAT-QuickHeal",
			          "engine_version": "22.00",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "McAfee": {
			          "category": "undetected",
			          "engine_name": "McAfee",
			          "engine_version": "6.0.6.653",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "Malwarebytes": {
			          "category": "undetected",
			          "engine_name": "Malwarebytes",
			          "engine_version": "4.5.5.54",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "Zillya": {
			          "category": "undetected",
			          "engine_name": "Zillya",
			          "engine_version": "2.0.0.4922",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230725"
			        },
			        "Paloalto": {
			          "category": "type-unsupported",
			          "engine_name": "Paloalto",
			          "engine_version": "0.9.0.1003",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "Sangfor": {
			          "category": "undetected",
			          "engine_name": "Sangfor",
			          "engine_version": "2.23.0.0",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230724"
			        },
			        "K7AntiVirus": {
			          "category": "undetected",
			          "engine_name": "K7AntiVirus",
			          "engine_version": "12.102.49078",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "Alibaba": {
			          "category": "type-unsupported",
			          "engine_name": "Alibaba",
			          "engine_version": "0.3.0.5",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20190527"
			        },
			        "K7GW": {
			          "category": "undetected",
			          "engine_name": "K7GW",
			          "engine_version": "12.102.49075",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "Trustlook": {
			          "category": "type-unsupported",
			          "engine_name": "Trustlook",
			          "engine_version": "1.0",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "Baidu": {
			          "category": "undetected",
			          "engine_name": "Baidu",
			          "engine_version": "1.0.0.2",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20190318"
			        },
			        "VirIT": {
			          "category": "undetected",
			          "engine_name": "VirIT",
			          "engine_version": "9.5.498",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230725"
			        },
			        "Cyren": {
			          "category": "undetected",
			          "engine_name": "Cyren",
			          "engine_version": "6.5.1.2",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "SymantecMobileInsight": {
			          "category": "type-unsupported",
			          "engine_name": "SymantecMobileInsight",
			          "engine_version": "2.0",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230119"
			        },
			        "Symantec": {
			          "category": "undetected",
			          "engine_name": "Symantec",
			          "engine_version": "1.20.0.0",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "Elastic": {
			          "category": "type-unsupported",
			          "engine_name": "Elastic",
			          "engine_version": "4.0.101",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230725"
			        },
			        "ESET-NOD32": {
			          "category": "undetected",
			          "engine_name": "ESET-NOD32",
			          "engine_version": "27633",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "APEX": {
			          "category": "type-unsupported",
			          "engine_name": "APEX",
			          "engine_version": "6.436",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230725"
			        },
			        "TrendMicro-HouseCall": {
			          "category": "undetected",
			          "engine_name": "TrendMicro-HouseCall",
			          "engine_version": "10.0.0.1040",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "Avast": {
			          "category": "undetected",
			          "engine_name": "Avast",
			          "engine_version": "22.11.7701.0",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "Cynet": {
			          "category": "undetected",
			          "engine_name": "Cynet",
			          "engine_version": "4.0.0.27",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "Kaspersky": {
			          "category": "undetected",
			          "engine_name": "Kaspersky",
			          "engine_version": "22.0.1.28",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "BitDefender": {
			          "category": "undetected",
			          "engine_name": "BitDefender",
			          "engine_version": "7.2",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "NANO-Antivirus": {
			          "category": "undetected",
			          "engine_name": "NANO-Antivirus",
			          "engine_version": "1.0.146.25796",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "SUPERAntiSpyware": {
			          "category": "undetected",
			          "engine_name": "SUPERAntiSpyware",
			          "engine_version": "5.6.0.1032",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230720"
			        },
			        "Tencent": {
			          "category": "undetected",
			          "engine_name": "Tencent",
			          "engine_version": "1.0.0.1",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "Emsisoft": {
			          "category": "undetected",
			          "engine_name": "Emsisoft",
			          "engine_version": "2022.6.0.32461",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "F-Secure": {
			          "category": "undetected",
			          "engine_name": "F-Secure",
			          "engine_version": "18.10.1137.128",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "DrWeb": {
			          "category": "undetected",
			          "engine_name": "DrWeb",
			          "engine_version": "7.0.59.12300",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "VIPRE": {
			          "category": "undetected",
			          "engine_name": "VIPRE",
			          "engine_version": "6.0.0.35",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "TrendMicro": {
			          "category": "undetected",
			          "engine_name": "TrendMicro",
			          "engine_version": "11.0.0.1006",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "McAfee-GW-Edition": {
			          "category": "undetected",
			          "engine_name": "McAfee-GW-Edition",
			          "engine_version": "v2021.2.0+4045",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "SentinelOne": {
			          "category": "type-unsupported",
			          "engine_name": "SentinelOne",
			          "engine_version": "23.3.0.3",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230705"
			        },
			        "Trapmine": {
			          "category": "type-unsupported",
			          "engine_name": "Trapmine",
			          "engine_version": "4.0.14.90",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230718"
			        },
			        "FireEye": {
			          "category": "undetected",
			          "engine_name": "FireEye",
			          "engine_version": "35.24.1.0",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "Sophos": {
			          "category": "undetected",
			          "engine_name": "Sophos",
			          "engine_version": "2.3.1.0",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "Ikarus": {
			          "category": "undetected",
			          "engine_name": "Ikarus",
			          "engine_version": "6.1.14.0",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "GData": {
			          "category": "undetected",
			          "engine_name": "GData",
			          "engine_version": "A:25.36259B:27.32549",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "Jiangmin": {
			          "category": "undetected",
			          "engine_name": "Jiangmin",
			          "engine_version": "16.0.100",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230725"
			        },
			        "Webroot": {
			          "category": "type-unsupported",
			          "engine_name": "Webroot",
			          "engine_version": "1.0.0.403",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "Avira": {
			          "category": "undetected",
			          "engine_name": "Avira",
			          "engine_version": "8.3.3.16",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "Antiy-AVL": {
			          "category": "undetected",
			          "engine_name": "Antiy-AVL",
			          "engine_version": "3.0",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "Gridinsoft": {
			          "category": "undetected",
			          "engine_name": "Gridinsoft",
			          "engine_version": "1.0.129.174",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "Xcitium": {
			          "category": "undetected",
			          "engine_name": "Xcitium",
			          "engine_version": "35856",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "Arcabit": {
			          "category": "undetected",
			          "engine_name": "Arcabit",
			          "engine_version": "2022.0.0.18",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "ViRobot": {
			          "category": "undetected",
			          "engine_name": "ViRobot",
			          "engine_version": "2014.3.20.0",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "ZoneAlarm": {
			          "category": "undetected",
			          "engine_name": "ZoneAlarm",
			          "engine_version": "1.0",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "Avast-Mobile": {
			          "category": "type-unsupported",
			          "engine_name": "Avast-Mobile",
			          "engine_version": "230726-02",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "Microsoft": {
			          "category": "undetected",
			          "engine_name": "Microsoft",
			          "engine_version": "1.1.23060.1005",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "Google": {
			          "category": "undetected",
			          "engine_name": "Google",
			          "engine_version": "1690369296",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "BitDefenderFalx": {
			          "category": "type-unsupported",
			          "engine_name": "BitDefenderFalx",
			          "engine_version": "2.0.936",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230724"
			        },
			        "AhnLab-V3": {
			          "category": "undetected",
			          "engine_name": "AhnLab-V3",
			          "engine_version": "3.24.0.10447",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "Acronis": {
			          "category": "undetected",
			          "engine_name": "Acronis",
			          "engine_version": "1.2.0.114",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230219"
			        },
			        "VBA32": {
			          "category": "undetected",
			          "engine_name": "VBA32",
			          "engine_version": "5.0.0",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "ALYac": {
			          "category": "undetected",
			          "engine_name": "ALYac",
			          "engine_version": "1.1.3.1",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "MAX": {
			          "category": "undetected",
			          "engine_name": "MAX",
			          "engine_version": "2023.1.4.1",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "DeepInstinct": {
			          "category": "type-unsupported",
			          "engine_name": "DeepInstinct",
			          "engine_version": "3.1.0.15",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230717"
			        },
			        "Cylance": {
			          "category": "type-unsupported",
			          "engine_name": "Cylance",
			          "engine_version": "2.0.0.0",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230719"
			        },
			        "Zoner": {
			          "category": "undetected",
			          "engine_name": "Zoner",
			          "engine_version": "2.2.2.0",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "Rising": {
			          "category": "undetected",
			          "engine_name": "Rising",
			          "engine_version": "25.0.0.27",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "Yandex": {
			          "category": "undetected",
			          "engine_name": "Yandex",
			          "engine_version": "5.5.2.24",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "TACHYON": {
			          "category": "undetected",
			          "engine_name": "TACHYON",
			          "engine_version": "2023-07-26.02",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "MaxSecure": {
			          "category": "undetected",
			          "engine_name": "MaxSecure",
			          "engine_version": "1.0.0.1",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230725"
			        },
			        "Fortinet": {
			          "category": "undetected",
			          "engine_name": "Fortinet",
			          "engine_version": "None",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "BitDefenderTheta": {
			          "category": "undetected",
			          "engine_name": "BitDefenderTheta",
			          "engine_version": "7.2.37796.0",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230713"
			        },
			        "AVG": {
			          "category": "undetected",
			          "engine_name": "AVG",
			          "engine_version": "22.11.7701.0",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230726"
			        },
			        "Cybereason": {
			          "category": "type-unsupported",
			          "engine_name": "Cybereason",
			          "engine_version": "1.2.449",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20210330"
			        },
			        "Panda": {
			          "category": "undetected",
			          "engine_name": "Panda",
			          "engine_version": "4.6.4.2",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20230725"
			        },
			        "CrowdStrike": {
			          "category": "type-unsupported",
			          "engine_name": "CrowdStrike",
			          "engine_version": "1.0",
			          "result": null,
			          "method": "blacklist",
			          "engine_update": "20220812"
			        }
			      }
			    },
			    "type": "analysis",
			    "id": "ZmVjN2ZmM2MxN2RlZTE0NjUxNTg1ZjMwMDY0NjEzZDE6MTY5MDM3MzczOQ==",
			    "links": {
			      "item": "https://www.virustotal.com/api/v3/files/3bf08e880ac1951de5980559e49e544546b918a7b60677c638e57a41b1927e11",
			      "self": "https://www.virustotal.com/api/v3/analyses/ZmVjN2ZmM2MxN2RlZTE0NjUxNTg1ZjMwMDY0NjEzZDE6MTY5MDM3MzczOQ=="
			    }
			  }
			}
		 * 
		 * 
		 */
	}


	/**
	 * 
	 * @return
	 */
	private static String getApiKey() {
		String apiKey = DIHelper.getInstance().getProperty(VIRUSTOTAL_API_KEY);
		if(apiKey == null || (apiKey=apiKey.trim()).isEmpty() ) {
			throw new NullPointerException("Must define the VIRUSTOTAL API KEY");
		}

		return apiKey;
	}

	/**
	 * 
	 * @return
	 */
	private static boolean useServerCert() {
		String useServerCert = DIHelper.getInstance().getProperty(VIRUSTOTAL_USE_CERT);
		if(useServerCert == null || (useServerCert=useServerCert.trim()).isEmpty() ) {
			return false;
		}

		return Boolean.parseBoolean(useServerCert);
	}
	
	/////////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////////
	

	
//	public static void main(String[] args) throws Exception {
//		TestUtilityMethods.loadDIHelper("C:/workspace/Semoss_Dev/RDF_Map.prop");
//		FileInputStream fs = new FileInputStream(new File("C:/Users/mahkhalil/Desktop/diabetes.csv"));
//		VirusTotalScannerUtils utils = new VirusTotalScannerUtils();
//		utils.getViruses("diabetes.csv", fs);
//	}
	
	
}
