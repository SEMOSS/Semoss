package prerna.reactor.model;

public class OAuthConfig {
	
	private String clientId;
    private String clientSecret;
    private String redirectUri;
    private String instanceUrl;
    private String scope;
    private String codeChallengeMethod;
    private String userinfoUrl;
    private String beanProps;
    private String jsonPattern;
    private String autoAdd;
    private String loginAllowed;
	public String getClientId() {
		return clientId;
	}
	public void setClientId(String clientId) {
		this.clientId = clientId;
	}
	public String getClientSecret() {
		return clientSecret;
	}
	public void setClientSecret(String clientSecret) {
		this.clientSecret = clientSecret;
	}
	public String getRedirectUri() {
		return redirectUri;
	}
	public void setRedirectUri(String redirectUri) {
		this.redirectUri = redirectUri;
	}
	public String getInstanceUrl() {
		return instanceUrl;
	}
	public void setInstanceUrl(String instanceUrl) {
		this.instanceUrl = instanceUrl;
	}
	public String getScope() {
		return scope;
	}
	public void setScope(String scope) {
		this.scope = scope;
	}
	public String getCodeChallengeMethod() {
		return codeChallengeMethod;
	}
	public void setCodeChallengeMethod(String codeChallengeMethod) {
		this.codeChallengeMethod = codeChallengeMethod;
	}
	public String getUserinfoUrl() {
		return userinfoUrl;
	}
	public void setUserinfoUrl(String userinfoUrl) {
		this.userinfoUrl = userinfoUrl;
	}
	public String getBeanProps() {
		return beanProps;
	}
	public void setBeanProps(String beanProps) {
		this.beanProps = beanProps;
	}
	public String getJsonPattern() {
		return jsonPattern;
	}
	public void setJsonPattern(String jsonPattern) {
		this.jsonPattern = jsonPattern;
	}
	public String getAutoAdd() {
		return autoAdd;
	}
	public void setAutoAdd(String autoAdd) {
		this.autoAdd = autoAdd;
	}
	public String getLoginAllowed() {
		return loginAllowed;
	}
	public void setLoginAllowed(String loginAllowed) {
		this.loginAllowed = loginAllowed;
	}
}
