package prerna.util.ldap;

public class LDAPPasswordChangeRequiredException extends RuntimeException {

	public LDAPPasswordChangeRequiredException() {
        super();
    }

    public LDAPPasswordChangeRequiredException(String message) {
        super(message);
    }

    public LDAPPasswordChangeRequiredException(String message, Throwable cause) {
        super(message, cause);
    }

    public LDAPPasswordChangeRequiredException(Throwable cause) {
        super(cause);
    }

    protected LDAPPasswordChangeRequiredException(String message, Throwable cause,
                               boolean enableSuppression,
                               boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
