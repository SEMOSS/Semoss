package prerna.io.connector.google;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public final class GoogleLoginUtils {
	
	private static final Logger classLogger = LogManager.getLogger(GoogleLoginUtils.class);
	
	private GoogleLoginUtils() {
		
	}
	
	/**
	 * 
	 * @param user
	 * @return
	 * @throws Exception
	 */
	public static String getGoogleAccessToken(User user) throws Exception {
		String accessToken = null;
		try {
			if (user == null) {
				Map<String, Object> retMap = new HashMap<>();
				retMap.put("type", "google");
				retMap.put("message", "Please login to your Google account");
				throwLoginError(retMap);
			} else {
				AccessToken googleToken = user.getAccessToken(AuthProvider.GOOGLE);
				accessToken = googleToken.getAccess_token();
			}
		} catch (Exception e) {
			Map<String, Object> retMap = new HashMap<>();
			retMap.put("type", "google");
			retMap.put("message", "Please login to your Google account");
			throwLoginError(retMap);
		}
		return accessToken;
	}
	
	/**
	 * 
	 * @param details
	 * @throws SemossPixelException
	 */
	public static void throwLoginError(Map<String, Object> details) throws SemossPixelException {
		SemossPixelException exception = new SemossPixelException(NounMetadata.getErrorNounMessage(details, PixelOperationType.LOGGIN_REQUIRED_ERROR));
		exception.setContinueThreadOfExecution(false);
		throw exception;
	}
	
	/**
	 * 
	 * @param googledriveUrl,accessToken
	 * @throws IOException
	 */
    public static JSONObject httpGetJson(String googledriveUrl, String accessToken) throws IOException {
        StringBuilder response = new StringBuilder();
        HttpURLConnection conn = null;
        BufferedReader in = null;

        try {
            URL url = new URL(googledriveUrl);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Authorization", "Bearer " + accessToken);

            // Set timeouts (10 seconds connect, 30 seconds read)
            conn.setConnectTimeout(10000);
            conn.setReadTimeout(30000);

            int responseCode = conn.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                StringBuilder errorMsg = new StringBuilder();
                InputStream errorStream = conn.getErrorStream();
                if (errorStream != null) {
                    try (BufferedReader errorReader = new BufferedReader(new InputStreamReader(errorStream))) {
                        String line;
                        while ((line = errorReader.readLine()) != null) {
                            errorMsg.append(line);
                        }
                    }
                } else {
                    errorMsg.append("No error details available.");
                }
                classLogger.error("HTTP GET failed: {} - {}", responseCode, errorMsg.toString());
                throw new IOException("HTTP GET failed with code: " + responseCode + " and body: " + errorMsg.toString());
            }

            in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            return new JSONObject(response.toString());

        } catch (SocketTimeoutException e) {
            classLogger.error(Constants.STACKTRACE, e);
            Map<String, Object> retMap=new HashMap<String, Object>();
            retMap.put("type", "SocketTimeoutException");
            retMap.put("message", "HTTP GET timed out");
            throw new IOException("SocketTimeoutException: "+retMap.toString(), e);
        } catch (IOException e) {
            classLogger.error(Constants.STACKTRACE, e);
            Map<String, Object> retMap=new HashMap<String, Object>();
            retMap.put("type", "IOException");
            retMap.put("message", e.getMessage());
            throw new IOException("IOException: "+retMap,e);
        } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
            throw new IOException("Unexpected error during HTTP GET", e);
        } finally {
            if (in != null) {
                try { in.close(); } catch (Exception ignore) {}
            }
            if (conn != null) {
                conn.disconnect();
            }
        }
    }
}
