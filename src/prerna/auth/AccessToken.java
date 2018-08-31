package prerna.auth;

public class AccessToken {

	AuthProvider provider = null;
	
	String id = null;
	String username = null;
	String access_token = null;
	int expires_in = 0; // this is in seconds
	String token_type = "Bearer";
	long startTime = -1;
	
	String email = null;
	String name = null;
	String profile = null;
	String gender = null;
	String locale = null;
	
	public void setAccess_token(String accessToken)
	{
		this.access_token = accessToken;
	}

	public String getAccess_token()
	{
		return this.access_token;
	}

	
	public void setExpires_in(int expires_in)
	{
		this.expires_in = expires_in;
	}
	
	public void setToken_type(String token_type)
	{
		this.token_type = token_type;
	}
	
	public void init()
	{
		startTime = System.currentTimeMillis();
	}

	public AuthProvider getProvider() {
		return provider;
	}

	public void setProvider(AuthProvider provider) {
		this.provider = provider;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public int getExpires_in() {
		return expires_in;
	}

	public String getToken_type() {
		return token_type;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getProfile() {
		return profile;
	}

	public void setProfile(String profile) {
		this.profile = profile;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public String getLocale() {
		return locale;
	}

	public void setLocale(String local) {
		this.locale = local;
	}

	public String getId() {
		if(id == null) {
			return email;
		}
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}
	
}
