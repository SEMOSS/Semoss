package prerna.io.connector;

import java.util.Map;

import prerna.auth.AccessToken;

public interface IAccessTokenFiller {

	/**
	 * 
	 * @param accessToken
	 * @param userInfoUrl
	 * @param jsonPattern
	 * @param beanProps
	 * @param params
	 */
	void fillAccessToken(AccessToken accessToken, String userInfoUrl, String jsonPattern, 
			String[] beanProps, Map<String, Object> params);
	
	/**
	 * 
	 * @param accessToken
	 * @param userInfoUrl
	 * @param jsonPattern
	 * @param beanProps
	 * @param params
	 * @param sanitizeResponse
	 */
	void fillAccessToken(AccessToken accessToken, String userInfoUrl, String jsonPattern, 
			String[] beanProps, Map<String, Object> params, boolean sanitizeResponse);

}
