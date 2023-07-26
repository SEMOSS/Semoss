package prerna.io.connector.antivirus;

import java.io.InputStream;
import java.util.Collection;
import java.util.Map;

public interface IVirusScanner {
	
	String CLAM_AV = "ClamAV";
	
	Map<String, Collection<String>> getViruses(String name, InputStream is);

}
