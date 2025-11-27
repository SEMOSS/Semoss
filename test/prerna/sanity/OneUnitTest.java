package prerna.sanity;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import prerna.SemossUnitTest;

import java.nio.file.Files;

public class OneUnitTest extends SemossUnitTest {

    @BeforeEach
    void setup() {
        System.out.println(tempDir.toAbsolutePath() + " " + Files.exists(tempDir));
    }


    @Test
    void test() {

    }

    @Test
    void test1() {

    }
}
