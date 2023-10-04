package prerna.util.linotp;

import java.io.IOException;
import java.io.StringReader;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CookieStore;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.cookie.ClientCookie;
import org.apache.http.impl.auth.DigestScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.util.Constants;
import prerna.util.SocialPropertiesUtil;
import prerna.util.ldap.ILdapAuthenticator;
import prerna.util.ldap.LDAPPasswordChangeRequiredException;

public class LinOTPUtil {

	private static final Logger classLogger = LogManager.getLogger(LinOTPUtil.class);

	/**
	 * 
	 * @param request
	 * @return
	 * @throws ClientProtocolException
	 * @throws IOException
	 */
	public static LinOTPResponse login(HttpServletRequest request) throws ClientProtocolException, IOException {
		SocialPropertiesUtil socialData = SocialPropertiesUtil.getInstance();

		Map<String, Object> returnMap = new HashMap<>();
		LinOTPResponse linotpResponse = new LinOTPResponse();
		linotpResponse.setReturnMap(returnMap);

		final String LINOTP_USERNAME = "username";
		final String LINOTP_TRANSACTION = "transactionId";
		final String OTP = "OTP";
		final String AD_ACCESS_TOKEN = "ad_token";
		
		// https://YOUR_LINOTP_SERVER/validate/check?user=USERNAME&pass=PINOTP
		final String hostname = socialData.getProperty("linotp_hostname");
		final String realm = socialData.getProperty("linotp_realm");

		String controller = "validate";
		String action = "check";
		String requestURL = hostname + "/" + controller + "/" + action;
		String username = request.getParameter("username");
		String pin = request.getParameter("pin");
		String otp = request.getParameter("otp");

		if( (username == null || username.isEmpty())
				&& (otp == null || otp.isEmpty()) ) {
			returnMap.put(Constants.ERROR_MESSAGE, "The user name cannot be null or empty.");
			linotpResponse.setResponseCode(401);
			return linotpResponse;
		}

		if( (pin == null || pin.isEmpty()) && (otp == null || otp.isEmpty())) {
			returnMap.put(Constants.ERROR_MESSAGE, "Must be providing either a pin or otp");
			linotpResponse.setResponseCode(401);
			return linotpResponse;
		}

		AccessToken adAuthToken = null;
		if (otp==null) {
			boolean checkAD = Boolean.parseBoolean(socialData.getProperty("linotp_check_ad", "false"));
			if(checkAD) {
				ILdapAuthenticator authenticator = null;
				try {
					authenticator = socialData.getLdapAuthenticator();
					adAuthToken = authenticator.authenticate(username, pin);
					if(adAuthToken == null) {
						throw new IllegalArgumentException("Unable to parse any user attributes");
					}
					
					// store this in the session
					HttpSession session = request.getSession();
					session.setAttribute(AD_ACCESS_TOKEN, adAuthToken);
				} catch(LDAPPasswordChangeRequiredException e) {
					HttpSession session = request.getSession(false);
					if(session != null) {
						User user = (User) session.getAttribute(Constants.SESSION_USER);
						if(!AbstractSecurityUtils.anonymousUsersEnabled() && user != null && user.getLogins().isEmpty()) {
							session.invalidate();
						}
					}
					classLogger.error(Constants.STACKTRACE, e);
					returnMap.put(Constants.ERROR_MESSAGE, e.getMessage());
					returnMap.put(ILdapAuthenticator.LDAP_PASSWORD_CHANGE_RETURN_KEY, true);
					linotpResponse.setResponseCode(401);
					return linotpResponse;
				} catch (Exception e) {
					HttpSession session = request.getSession(false);
					if(session != null) {
						User user = (User) session.getAttribute(Constants.SESSION_USER);
						if(!AbstractSecurityUtils.anonymousUsersEnabled() && user != null && user.getLogins().isEmpty()) {
							session.invalidate();
						}
					}
					classLogger.error(Constants.STACKTRACE, e);
					returnMap.put(Constants.ERROR_MESSAGE, "Unable to authenticate with active directory");
					linotpResponse.setResponseCode(500);
					return linotpResponse;
				} finally {
					if(authenticator != null) {
						try {
							authenticator.close();
						} catch (IOException e) {
							classLogger.error(Constants.STACKTRACE, e);
						}
					}
				}
			}

			// first, request for challenge request using user pin
			// Create HTTP request via ssl port (https) and pass post parameters
			CloseableHttpClient httpclient = HttpClients
					.custom()
					.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
					.build();
			try {
				HttpEntity entity = null;
				HttpPost httpPost = new HttpPost(requestURL);
				List <NameValuePair> nvps = new ArrayList <NameValuePair>();
				nvps.add(new BasicNameValuePair("user", username));
				nvps.add(new BasicNameValuePair("pass", pin));
				nvps.add(new BasicNameValuePair("realm", realm));
				httpPost.setEntity(new UrlEncodedFormEntity(nvps));
				CloseableHttpResponse postResponse = httpclient.execute(httpPost);
				try {
					entity = postResponse.getEntity();
					String s_response = EntityUtils.toString(entity);
					JsonReader reader = Json.createReader(new StringReader(s_response));
					JsonObject j_response = reader.readObject();
					//parse json response for result value
					JsonObject j_result = j_response.getJsonObject("result");
					Boolean authenticated = j_result.getBoolean("value", false);
					if (authenticated) {
						// this means we have authenticated
						// and there is no policy requiring an otp
						// just the initial login is being performed where that
						// pin can be defined as the ldap password or the otp pin
						AccessToken token = new AccessToken();
						token.setProvider(AuthProvider.LINOTP);
						token.setId(username);
						token.setUsername(username);
						returnMap.put("success", "true");
						returnMap.put("username", username);
						
						// if we did an AD login might as well grab those properties
						HttpSession session = request.getSession();
						adAuthToken = (AccessToken) session.getAttribute(AD_ACCESS_TOKEN);
						if(adAuthToken != null) {
							token.setEmail(adAuthToken.getEmail());
							token.setLastPasswordReset(adAuthToken.getLastPasswordReset());
							session.removeAttribute(AD_ACCESS_TOKEN);
						}
						
						linotpResponse.setToken(token);
						linotpResponse.setResponseCode(200);
						
						// this is just an attempt to reset counter
						// still allow user to login
						try {
							resetCounter(new LinOTPResponse(), username);
						} catch(Exception e) {
							classLogger.error(Constants.STACKTRACE, e);
						}
						
						return linotpResponse;
					} else {
						// challenge request flow
						if (j_response.containsKey("detail")) {
							JsonObject j_detail = j_response.getJsonObject("detail");
							String transactionId = j_detail.getString("transactionid");
							HttpSession session = request.getSession();
							session.setAttribute(LINOTP_USERNAME, username);
							session.setAttribute(LINOTP_TRANSACTION, transactionId);
							returnMap.put(OTP, "Please get OTP code.");
							linotpResponse.setResponseCode(200);
							return linotpResponse;
						} else {
							returnMap.put(Constants.ERROR_MESSAGE, "The user name or pin/password are invalid.");
							linotpResponse.setResponseCode(401);
							return linotpResponse;
						}
					}
					
				} finally {
					// consume will release the entity
					if(entity != null) {
						EntityUtils.consume(entity);
					}
					if(postResponse != null) {
						postResponse.close();
					}
				}
			} finally {
				if(httpclient != null) {
					httpclient.close();
				}
			}
			
		} else {
			// subsequent challenge request with otp
			// Create HTTP request via ssl port (https) and pass post parameters
			CloseableHttpClient httpclient = HttpClients
					.custom()
					.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
					.build();
			try {
				HttpPost httpPost = new HttpPost(requestURL);
				HttpSession session = request.getSession();
				username = (String) session.getAttribute(LINOTP_USERNAME);
				String transactionId = (String) session.getAttribute(LINOTP_TRANSACTION);
				if(username == null || username.isEmpty() || transactionId == null || transactionId.isEmpty()) {
					returnMap.put(Constants.ERROR_MESSAGE, "The user must re-enter their username and password before proceeding to enter their 2FA pin");
					linotpResponse.setResponseCode(401);
					return linotpResponse;
				}

				List <NameValuePair> nvps = new ArrayList <NameValuePair>();
				nvps.add(new BasicNameValuePair("user", username));
				nvps.add(new BasicNameValuePair("pass", otp));
				nvps.add(new BasicNameValuePair("realm", realm));
				nvps.add(new BasicNameValuePair("transactionid", transactionId));
				httpPost.setEntity(new UrlEncodedFormEntity(nvps));
				CloseableHttpResponse postResponse = httpclient.execute(httpPost);
				HttpEntity entity = null;
				try {
					entity = postResponse.getEntity();
					String s_response = EntityUtils.toString(entity);
					JsonReader reader = Json.createReader(new StringReader(s_response));
					JsonObject j_response = reader.readObject();
					//parse json response for result value
					JsonObject j_result = j_response.getJsonObject("result");
					Boolean authenticated = j_result.getBoolean("value", false);

					if (authenticated) {
						AccessToken token = new AccessToken();
						token.setProvider(AuthProvider.LINOTP);
						token.setId(username);
						token.setUsername(username);
						
						// if we did an AD login might as well grab those properties
						adAuthToken = (AccessToken) session.getAttribute(AD_ACCESS_TOKEN);
						if(adAuthToken != null) {
							token.setEmail(adAuthToken.getEmail());
							token.setLastPasswordReset(adAuthToken.getLastPasswordReset());
							session.removeAttribute(AD_ACCESS_TOKEN);
						}
						
						returnMap.put("success", "true");
						returnMap.put("username", username);
						
						linotpResponse.setToken(token);
						linotpResponse.setResponseCode(200);
						
						// this is just an attempt to reset counter
						// still allow user to login
						try {
							resetCounter(new LinOTPResponse(), username);
						} catch(Exception e) {
							classLogger.error(Constants.STACKTRACE, e);
						}
						
						return linotpResponse;
					}
					else {
						returnMap.put(Constants.ERROR_MESSAGE, "The username or one-time passcode are invalid.");
						linotpResponse.setResponseCode(401);
						return linotpResponse;
					}
				} finally {
					// consume will release the entity
					if(entity != null) {
						EntityUtils.consume(entity);
					}
					if(postResponse != null) {
						postResponse.close();
					}
				}
			} finally {
				if(httpclient != null) {
					httpclient.close();
				}
			}
		}
	}
	
	/**
	 * 
	 * @param request
	 * @return
	 * @throws ClientProtocolException
	 * @throws IOException
	 */
	public static LinOTPResponse resetCounter(HttpServletRequest request) throws ClientProtocolException, IOException {
		User thisUser = (User) request.getSession().getAttribute(Constants.SESSION_USER);
		String username = request.getParameter("username");
		return resetCounter(thisUser, username);
	}
	
	/**
	 * 
	 * @param thisUser
	 * @param username
	 * @return
	 * @throws ClientProtocolException
	 * @throws IOException
	 */
	public static LinOTPResponse resetCounter(User thisUser, String username) throws ClientProtocolException, IOException {
		Map<String, Object> returnMap = new HashMap<>();
		LinOTPResponse linotpResponse = new LinOTPResponse();
		linotpResponse.setReturnMap(returnMap);
		
		if(thisUser == null) {
			returnMap.put(Constants.ERROR_MESSAGE, "User must be logged in to invoke this endpoint");
			linotpResponse.setResponseCode(500);
			return linotpResponse;
		}
		if(SecurityAdminUtils.getInstance(thisUser) == null) {
			returnMap.put(Constants.ERROR_MESSAGE, "User must be an admin");
			linotpResponse.setResponseCode(500);
			return linotpResponse;
		}
		
		return resetCounter(linotpResponse, username);
	}
	
	/**
	 * 
	 * @param linotpResponse
	 * @param username
	 * @return
	 * @throws ClientProtocolException
	 * @throws IOException
	 */
	private static LinOTPResponse resetCounter(LinOTPResponse linotpResponse, String username) throws ClientProtocolException, IOException {
		SocialPropertiesUtil socialData = SocialPropertiesUtil.getInstance();
		Map<String, Object> returnMap = linotpResponse.getReturnMap();
		
		if(socialData.getLoginsAllowed().get("linotp")==null || !socialData.getLoginsAllowed().get("linotp")) {
			returnMap.put(Constants.ERROR_MESSAGE, "LinOTP login is not allowed");
			linotpResponse.setResponseCode(400);
			return linotpResponse;
		}

		final String adminUser = socialData.getProperty("linotp_adminuser");
		final String adminPass = socialData.getProperty("linotp_adminpassword");

		if(adminUser == null || adminPass == null || adminUser.isEmpty() || adminPass.isEmpty()) {
			returnMap.put(Constants.ERROR_MESSAGE, "Admin user/pass is not setup to invoke this endpoint");
			linotpResponse.setResponseCode(500);
			return linotpResponse;
		}

		// https://YOUR_LINOTP_SERVER/admin/reset?user=USERNAME
		final String hostname = socialData.getProperty("linotp_hostname"); 
		final String realm = socialData.getProperty("linotp_realm");

		String cleanHostname = hostname;
		if(cleanHostname.startsWith("https://")) {
			cleanHostname = cleanHostname.substring("https://".length());
		} else if(cleanHostname.startsWith("http://")) {
			cleanHostname = cleanHostname.substring("http://".length());
		}
		
		String controller = "admin";
		String action = "reset";
		String requestURL = hostname + "/" + controller + "/" + action;

		if( username == null || username.isEmpty() ) {
			returnMap.put(Constants.ERROR_MESSAGE, "The user name cannot be null or empty.");
			linotpResponse.setResponseCode(401);
			return linotpResponse;
		}

		SecureRandom random = new SecureRandom();
		byte bytes[] = new byte[32];
		random.nextBytes(bytes);
		Encoder encoder = Base64.getUrlEncoder().withoutPadding();
		String token = encoder.encodeToString(bytes);
		
		// LinOTP does not check user authorization when accessing the administrative API
		// but relies on the web server running LinOTP (as a WSGI app) to do this.
		// context is passed into the httpclient.execute() method
		HttpClientContext context = HttpClientContext.create();
		CredentialsProvider credsProvider = new BasicCredentialsProvider();
		credsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(adminUser, adminPass));
		AuthCache authCache = new BasicAuthCache();
		authCache.put(new HttpHost(cleanHostname), new DigestScheme());
		context.setAuthCache(authCache);
		context.setCredentialsProvider(credsProvider);
		
		// must send the token in the cookie as well as the session key in the body
		CookieStore cstore = new BasicCookieStore();
		BasicClientCookie cookie = new BasicClientCookie("admin_session", token);
		cookie.setDomain(cleanHostname);
	    cookie.setAttribute(ClientCookie.DOMAIN_ATTR, "true");
		cstore.addCookie(cookie);
		context.setCookieStore(cstore);
		
		// Create HTTP request via ssl port (https) and pass post parameters
		CloseableHttpClient httpclient = HttpClients
				.custom()
				.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
				.build();
		try {
			HttpPost httpPost = new HttpPost(requestURL);

			List <NameValuePair> nvps = new ArrayList <NameValuePair>();
			nvps.add(new BasicNameValuePair("user", username));
			nvps.add(new BasicNameValuePair("realm", realm));
			nvps.add(new BasicNameValuePair("session", token));
			httpPost.setEntity(new UrlEncodedFormEntity(nvps));
			CloseableHttpResponse postResponse = httpclient.execute(httpPost, context);
			HttpEntity entity = null;
			try {
				entity = postResponse.getEntity();
				String s_response = EntityUtils.toString(entity);
				JsonReader reader = Json.createReader(new StringReader(s_response));
				JsonObject j_response = reader.readObject();
				//parse json response for result value
				JsonObject j_result = j_response.getJsonObject("result");
				Integer validReset = j_result.getInt("value", 0);

				if (validReset == 1) {
					returnMap.put("success", "true");
					returnMap.put("message", "Successful reset");
					linotpResponse.setResponseCode(200);
					return linotpResponse;
				} else {
					JsonObject detailJson = j_response.getJsonObject("detail");
					if(detailJson != null) {
						String errorMessage = detailJson.getString("message");
						if(errorMessage != null) {
							returnMap.put(Constants.ERROR_MESSAGE, "Unsuccessful reset - " + errorMessage);
							linotpResponse.setResponseCode(500);
							return linotpResponse;
						}
					}
					
					JsonObject resultJson = j_response.getJsonObject("result");
					if(resultJson != null) {
						JsonObject errorJson = resultJson.getJsonObject("error");
						if(errorJson != null) {
							String errorMessage = errorJson.getString("message");
							if(errorMessage != null) {
								returnMap.put(Constants.ERROR_MESSAGE, "Unsuccessful reset - " + errorMessage);
								linotpResponse.setResponseCode(500);
								return linotpResponse;
							}
						}
					}
					
					// could not parse a specific message - return json response for debugging
					returnMap.put(Constants.ERROR_MESSAGE, "Unsuccessful reset - unable to parse error details. Full error: " + j_response.toString());
					linotpResponse.setResponseCode(500);
					return linotpResponse;
				}
			} finally {
				// consume will release the entity
				if(entity != null) {
					EntityUtils.consume(entity);
				}
				if(postResponse != null) {
					postResponse.close();
				}
			}
		} finally {
			if(httpclient != null) {
				httpclient.close();
			}
		}
	}
	
}
