package prerna.auth;

public enum AuthProvider {

	    GOOGLE, FACEBOOK, TWITTER, SALESFORCE, GIT, AZURE_GRAPH, DROPBOX, CAC, PRODUCT_HUNT, IN, GOOGLE_MAP, NATIVE;

	    public String toString() {
	        return name().charAt(0) + name().substring(1).toLowerCase();
	    }
	
}
