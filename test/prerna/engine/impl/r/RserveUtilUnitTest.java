package prerna.engine.impl.r;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Paths;

import org.junit.jupiter.api.Test;


public class RserveUtilUnitTest {

    @Test
    public void testGetRDataFileSimpleName() {
        String result = RserveUtil.getRDataFile("myfile");

        assertNotNull(result);
        assertTrue(result.endsWith("myfile.RData"));
    }

    @Test
    public void testGetRDataFileWithRootAndName() {
        String root = Paths.get("/tmp/testroot").toString();
        String result = RserveUtil.getRDataFile(root, "data123");

        assertNotNull(result);
        assertTrue(result.replace('\\', '/').endsWith("/data123.RData"));
    }

}
