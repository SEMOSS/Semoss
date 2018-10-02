package prerna.auth;

public enum AuthProvider {

	GOOGLE, 
	GOOGLE_MAP,
	GITHUB,
	MS, // this is azure graph
	SF, // this is salesforce 
	NATIVE,

	// this one is kinda special ...
	CAC, 

	// these are not used as much ...
	FACEBOOK, 
	TWITTER, 
	DROPBOX, 
	PRODUCT_HUNT, 
	IN, 
	;

	public String toString() {
		return name().charAt(0) + name().substring(1).toLowerCase();
	}

}
