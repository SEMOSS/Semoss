package prerna.testing;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class EngineTestConfigurationUnitTests {
    @TempDir Path folder;
    private Map<String,Object> profile(String real, String test) {
        return Map.of("name", "Orders fixtures", "mappings", List.of(Map.of("realEngineId",real,"testEngineId",test)));
    }
    @Test void roundTripsIdentityAndMappingsAndForcesUnmappedDatabasesToBeDenied() {
        var profile = EngineTestConfiguration.normalize(profile("real", "test"));
        assertEquals(Map.of("real", "test"), EngineTestConfiguration.mappings(profile));
        assertEquals(true, profile.get("denyUnmappedDatabases"));
        EngineTestConfiguration.write(folder.resolve("profile.json"), profile);
        assertEquals(profile.get("id"), EngineTestConfiguration.read(folder.resolve("profile.json")).get("id"));
        assertEquals(Map.of("real","test"), EngineTestConfiguration.mappings(EngineTestConfiguration.read(folder.resolve("profile.json"))));
    }
    @Test void distinguishesAbsenceFromCorruption() throws Exception {
        Path file = folder.resolve("profile.json");
        assertNull(EngineTestConfiguration.read(file));
        Files.writeString(file, "not JSON");
        assertThrows(IllegalArgumentException.class, () -> EngineTestConfiguration.read(file));
    }
    @Test void rejectsSameEngineChainedMappingsAndUnsupportedVersions() {
        assertThrows(IllegalArgumentException.class, () -> EngineTestConfiguration.normalize(profile("same", "same")));
        assertThrows(IllegalArgumentException.class, () -> EngineTestConfiguration.normalize(Map.of("mappings",List.of(
            Map.of("realEngineId","a","testEngineId","b"), Map.of("realEngineId","b","testEngineId","c")))));
        assertThrows(IllegalArgumentException.class, () -> EngineTestConfiguration.normalize(Map.of("version",1.5,"mappings",profile("a","b").get("mappings"))));
    }
    @Test void rejectsRemoteMemoryAndExecutableConnectionOptions() {
        for (String url : List.of("jdbc:h2:tcp://prod/db", "jdbc:h2:mem:test", "jdbc:sqlite::memory:", "jdbc:h2:file:/tmp/mock;INIT=RUNSCRIPT FROM 'prod.sql'", "jdbc:sqlite:file:/tmp/prod?mode=rw"))
            assertThrows(IllegalArgumentException.class, () -> EngineTestConfiguration.localFile(url), url);
        assertEquals(Path.of("/tmp/mock"), EngineTestConfiguration.localFile("jdbc:h2:nio:/tmp/mock;query_timeout=180000;cache_size=32768"));
    }
}
