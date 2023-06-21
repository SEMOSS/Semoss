package prerna.io.connector.antivirus;

import java.io.IOException;

public class VirusScanningException extends IOException {

    public VirusScanningException() {
        super();
    }

    public VirusScanningException(String message) {
        super(message);
    }

    public VirusScanningException(String message, Throwable cause) {
        super(message, cause);
    }

    public VirusScanningException(Throwable cause) {
        super(cause);
    }
	
}
