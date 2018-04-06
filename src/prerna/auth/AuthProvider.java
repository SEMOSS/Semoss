package prerna.auth;

public enum AuthProvider {

	    GOOGLE, FACEBOOK, TWITTER, SALESFORCE, GIT, AZURE_GRAPH, DROPBOX, CAC;

	    public String toString() {
	        return name().charAt(0) + name().substring(1).toLowerCase();
	    }
	
}
