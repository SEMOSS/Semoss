package prerna.engine.impl.owl;

import org.junit.jupiter.api.Test;
import prerna.engine.impl.rdf.RDFFileSesameEngine;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

public class ReadOnlyOWLEngineUnitTests {

    @Test
    void testGetDatabaseEngine() {
        RDFFileSesameEngine sesameEngine = mock(RDFFileSesameEngine.class);
        ReadOnlyOWLEngine engine = new ReadOnlyOWLEngine(sesameEngine, "eid", "ename");
        assertNull(engine.getBaseDataEngine());
    }

    @Test
    void testSetDatabaseEngine() {
        RDFFileSesameEngine sesameEngine = mock(RDFFileSesameEngine.class);
        ReadOnlyOWLEngine engine = new ReadOnlyOWLEngine(sesameEngine, "eid", "ename");
        engine.setBaseDataEngine(sesameEngine);
        assertNull(engine.getBaseDataEngine());
    }
}
