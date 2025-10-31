package prerna;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class SemossUnitTest {

    protected static Path tempDir;
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
        }

        if (!Files.exists(tempDir)) {
            Files.createDirectories(tempDir);
        }
    }

    @AfterAll
    static void tearDown() throws IOException {
        if (Files.exists(tempDir)) {
            FileUtils.cleanDirectory(tempDir.toFile());
        }
    }

}
