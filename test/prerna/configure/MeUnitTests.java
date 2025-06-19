package prerna.configure;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

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

        temp = tempDir.resolve(tempDir.resolve("tomcat"));
        Files.createDirectory(temp);
        confDir = tempDir.resolve(tempDir.resolve(temp + "\\conf"));
        Files.createDirectory(confDir);
        bin = tempDir.resolve(temp + "\\bin");
        Files.createDirectories(bin);
        temp = tempDir.resolve(tempDir.resolve(temp + "\\webapps"));
        Files.createDirectory(temp);
        temp = tempDir.resolve(tempDir.resolve(temp + "\\Monolith"));
        Files.createDirectory(temp);
        webINF = tempDir.resolve(tempDir.resolve(temp + "\\WEB-INF"));
        Files.createDirectory(webINF);

        temp = tempDir.resolve(homePath + "\\config");
        Files.createDirectories(temp);
        openBrowser = tempDir.resolve(temp + "\\openBrowser.bat");
        openBrowser = Files.createFile(openBrowser);

        configured = tempDir.resolve(homePath + "\\configured.txt");
        configured = Files.createFile(configured);

        setPath = tempDir.resolve(homePath + "\\setPath.bat");
        setPath = Files.createFile(setPath);

        setenv = tempDir.resolve(bin + "\\setenv.bat");
        setenv = Files.createFile(setenv);

        rdfTemp = tempDir.resolve(homePath + "\\RDF_Map.prop");
        URL url = Me.class.getResource("test_RDF_Map.prop");
        Path p = Paths.get(url.toURI());
        rdfTemp = Files.copy(p, rdfTemp);

        webxmlTemp = tempDir.resolve(webINF + "\\web.xml");
        url = Me.class.getResource("test_web.xml");
        p = Paths.get(url.toURI());
        webxmlTemp = Files.copy(p, webxmlTemp);

        server = tempDir.resolve(confDir + "\\server.xml");
        url = Me.class.getResource("test_server.xml");
        p = Paths.get(url.toURI());
        server = Files.copy(p, server);

        temp = tempDir;
    }
    
    @Test
    void mainTestNull(@TempDir Path temp) throws Exception {
            reactor.main(null);
    }

    @Test
    void mainTest() throws Exception {
        String tempDirString = temp.toAbsolutePath().toString().replace('\\', '/');
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
			assertTrue(p.get(x).toString().startsWith(homePath.toAbsolutePath().toString().replace('\\', '/')));
		}

        Scanner scn = new Scanner(webxmlTemp);

        // Checks resulting web.xml
        while (scn.hasNextLine()) {
            String str = scn.nextLine();
            if (str.trim().startsWith("<param-value>")) {
                assertTrue(str.contains(rdfTemp.toAbsolutePath().toString().replace('\\', '/')));
            }

            continue;
        }
        scn.close();

        // Checks resulting setPath.bat
        scn = new Scanner(setPath);
        while (scn.hasNextLine()) {
            String str = scn.nextLine();

            if (!str.toLowerCase().startsWith("set"))    continue;

            // shortens string to first index of temp dir until end, splits string by ; to test each indiv path in string
            str = str.substring(str.indexOf(tempDirString));
            String[] arr = str.split(";");
            for (String s : arr) {
                assertTrue(s.contains(tempDirString));
            }
        }
        scn.close();

        // Checks resulting setenv.bat
        scn = new Scanner(setenv);
        while (scn.hasNextLine()) {
            String str = scn.nextLine();
            str = str.substring(str.indexOf(tempDirString));

            String[] arr = str.split(";");
            for (String s: arr)
                assertTrue(s.contains(tempDirString));
        }
        scn.close();

        // Checks resulting server.xml
        String portNum = "";
        scn = new Scanner(server);
        while (scn.hasNextLine()) {
            String str = scn.nextLine();

            if (str.trim().startsWith("<?xml")) {
                assertTrue(str.equalsIgnoreCase("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><!--"));
            } else if (str.trim().startsWith("<Connector connectionTimeout=\"20000\"")) {
                str = str.substring(str.indexOf("port"), str.indexOf(" protocol"));
                portNum = str.substring(str.indexOf("\"") + 1, str.lastIndexOf("\""));
                break;
            }
        }
        scn.close();

        if(portNum.isEmpty())    return;

        // Checks resulting openBrowser.bat
        scn = new Scanner(openBrowser);
        while (scn.hasNextLine()) {
            String str = scn.nextLine();
            if (str.equals("ECHO OFF") || str.isEmpty()) continue;

            assertTrue(str.contains("http://localhost:" + portNum + "/SemossWeb/"));
        }
        scn.close();

        // Checks resulting configured.txt
        scn = new Scanner(configured);
        if (scn.hasNextLine()) {
            String str = scn.nextLine();
            String s = str.substring(str.indexOf("= "));
            assertTrue(str.substring(str.indexOf("= ") + 2).startsWith(portNum));
            assertTrue(str.contains("http://localhost:" + portNum + "/SemossWeb/"));
        }
        scn.close();
    }
}