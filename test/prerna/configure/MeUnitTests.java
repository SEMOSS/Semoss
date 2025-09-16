package prerna.configure;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class MeUnitTests {
    Me reactor;
    Path temp, homePath, rHome, rLib, rdll, jriDll, webINF, confDir, bin,
        rdfTemp, webxmlTemp, server, openBrowser, configured, setPath, setenv;

    @BeforeEach
    void setup(@TempDir Path tempDir) throws Exception {
        reactor = new Me();

        homePath = tempDir.resolve("semoss");
        Files.createDirectory(homePath);

        rHome = tempDir.resolve("r");
        Files.createDirectory(rHome);

        rLib = tempDir.resolve("rLib");
        Files.createDirectory(rLib);

        rdll = tempDir.resolve("rDll");
        Files.createDirectory(rdll);

        jriDll = tempDir.resolve("jri");
        Files.createDirectory(jriDll);

        temp = tempDir.resolve("tomcat");
        Files.createDirectory(temp);
        confDir = tempDir.resolve(temp + File.separator + "conf");
        Files.createDirectory(confDir);
        bin = tempDir.resolve(temp + File.separator + "bin");
        Files.createDirectories(bin);
        temp = tempDir.resolve(temp + File.separator + "webapps");
        Files.createDirectory(temp);
        temp = tempDir.resolve(temp + File.separator + "Monolith");
        Files.createDirectory(temp);
        webINF = tempDir.resolve(temp + File.separator + "WEB-INF");
        Files.createDirectory(webINF);

        temp = tempDir.resolve(homePath + File.separator + "config");
        Files.createDirectories(temp);
        openBrowser = tempDir.resolve(temp + File.separator + "openBrowser.bat");
        openBrowser = Files.createFile(openBrowser);

        configured = tempDir.resolve(homePath + File.separator + "configured.txt");
        configured = Files.createFile(configured);

        setPath = tempDir.resolve(homePath + File.separator + "setPath.bat");
        setPath = Files.createFile(setPath);

        setenv = tempDir.resolve(bin + File.separator + "setenv.bat");
        setenv = Files.createFile(setenv);

        rdfTemp = tempDir.resolve(homePath + File.separator + "RDF_Map.prop");
        URL url = Me.class.getResource("test_RDF_Map.prop");
        Path p = Paths.get(url.toURI());
        rdfTemp = Files.copy(p, rdfTemp);

        webxmlTemp = tempDir.resolve(webINF + File.separator + "web.xml");
        url = Me.class.getResource("test_web.xml");
        p = Paths.get(url.toURI());
        webxmlTemp = Files.copy(p, webxmlTemp);

        server = tempDir.resolve(confDir + File.separator + "server.xml");
        url = Me.class.getResource("test_server.xml");
        p = Paths.get(url.toURI());
        server = Files.copy(p, server);

        temp = tempDir;
    }
    
    @Test
    void mainTest() throws Exception {
        int idx = 0;
        String tempDirString = temp.toAbsolutePath().toString().replace(File.separator, "/");
        String[] args = {
            homePath.toAbsolutePath().toString(), 
            rHome.toAbsolutePath().toString(), 
            rLib.toAbsolutePath().toString(), 
            rdll.toAbsolutePath().toString(), 
            jriDll.toAbsolutePath().toString()
        };

        reactor.main(args);

        // Checks resulting RDF_Map.prop
        Properties p = new Properties();
		p.load(Files.newInputStream(rdfTemp));
		List<String> rdfKeys = new ArrayList<String>() {{
            add("BaseFolder");
            add("SMSSWebWatcher_DIR");
            add("SMSSWatcher_DIR");
            add("SMSSStorageWatcher_DIR");
            add("SMSSModelWatcher_DIR");
            add("SMSSVectorWatcher_DIR");
            add("SMSSFunctionWatcher_DIR");
            add("SMSSGuardrailWatcher_DIR");
            add("SMSSVenvWatcher_DIR");
            add("ProjectWatcher_DIR");
            add("INSIGHT_CACHE_DIR");
            add("ADDITIONAL_REACTORS");
            add("SOCIAL");
            add("JobSchedulerWatcher_DIR");
            add("rpa.config.directory");
            add("EMAIL_TEMPLATES");
        }};

		for (String x : rdfKeys) {
			assertTrue(p.getProperty(x).startsWith(homePath.toAbsolutePath().toString().replace(File.separator, "/")));
		}

        // Checks resulting web.xml
        List<String> lines = Files.readAllLines(webxmlTemp);
        assertTrue(lines.get(3).contains(rdfTemp.toAbsolutePath().toString().replace(File.separator, "/")));

        // Checks resulting setPath.bat
        lines = Files.readAllLines(setPath);
        assertTrue(lines.get(2).contains(tempDirString));
        assertTrue(lines.get(3).contains(tempDirString));
        assertTrue(lines.get(5).contains(tempDirString));

        // Checks resulting setenv.bat
        lines = Files.readAllLines(setenv);
        assertTrue(lines.get(0).contains(tempDirString));

        // Checks resulting server.xml
        lines = Files.readAllLines(server);
        assertTrue(lines.get(0).startsWith("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>"));
        
        String tempString = lines.get(2).trim();
        idx = tempString.indexOf("port");
        tempString = idx >= 0 ? tempString.substring(idx + 6) : ""; // String looks like <PORT_NUMBER>" protocol="...
        idx = tempString.indexOf(" protocol");
        tempString = idx >= 0 ? tempString.substring(0, idx) : "";  // String looks like <PORT_NUMBER>"
        String portNum = (!tempString.equals("")) ? tempString.substring(0, tempString.indexOf("\"")): "";

        if(portNum.isEmpty())    return;

        // Checks resulting openBrowser.bat 04
        lines = Files.readAllLines(openBrowser);
        assertTrue(lines.get(0).contains("http://localhost:" + portNum + "/SemossWeb/"));
        assertTrue(lines.get(4).contains("http://localhost:" + portNum + "/SemossWeb/"));

        // Checks resulting configured.txt 0
        lines = Files.readAllLines(configured);
        assertTrue(lines.get(0).contains(portNum));
        assertTrue(lines.get(0).contains("http://localhost:" + portNum + "/SemossWeb/"));

    }
}