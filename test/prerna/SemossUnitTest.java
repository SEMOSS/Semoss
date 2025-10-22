package prerna;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import prerna.util.Constants;
import prerna.util.DIHelper;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public class SemossUnitTest {


    protected static Path tempDir;

    protected static Path semossDir;
    protected static Path insightCacheDir;
    protected static Path projectDir;

    private static boolean first = false;

    @TempDir
    static Path firstIsTempDirTheRestAreImpostersToNotBeUsed;

    @BeforeAll
    static void setup() throws IOException {
        if (!first) {
            System.out.println("Run Once");
            tempDir = firstIsTempDirTheRestAreImpostersToNotBeUsed;
            first = true;
            // Pointing Semoss to Use this temp directory
            Properties prop = setupProperties(tempDir);
            DIHelper.getInstance().setCoreProp(prop);
        }

        System.out.println("Temp dir: " + tempDir);
        if (!Files.exists(tempDir)) {
            Files.createDirectories(tempDir);
        }
    }

    private static Properties setupProperties(Path tempDir) throws IOException {
        Properties prop = new Properties();

        semossDir = tempDir.resolve("semoss");

        insightCacheDir = semossDir.resolve("insightCache");
        projectDir = semossDir.resolve("project");
        Files.createDirectories(projectDir);

        prop.setProperty(Constants.BASE_FOLDER, semossDir.toString());
        prop.setProperty(Constants.INSIGHT_CACHE_DIR, insightCacheDir.toString());
        prop.setProperty(Constants.PROJECT_FOLDER, projectDir.toString());

        return prop;
    }

    @AfterAll
    static void tearDown() throws IOException {
        if (Files.exists(tempDir)) {
            FileUtils.cleanDirectory(tempDir.toFile());
        }
    }

}
