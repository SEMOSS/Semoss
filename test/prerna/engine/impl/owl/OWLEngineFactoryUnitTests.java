package prerna.engine.impl.owl;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.util.Utility;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;

public class OWLEngineFactoryUnitTests {

    private OWLEngineFactory factory;

    @Test
    void testGetReadOWL() {

        try (MockedStatic<Utility> utilityMockedStatic = Mockito.mockStatic(Utility.class)) {
            RDFFileSesameEngine rfse = mock(RDFFileSesameEngine.class);

            Vector<String> empty = new Vector<>();
            utilityMockedStatic.when(() -> Utility.getVectorOfReturn(anyString(), eq(rfse), eq(true))).thenReturn(empty);
            Vector<String[]> emptyArray = new Vector<>();
            utilityMockedStatic.when(() -> Utility.getVectorArrayOfReturn(anyString(), eq(rfse), eq(true))).thenReturn(emptyArray);

            factory = new OWLEngineFactory(rfse, IDatabaseEngine.DATABASE_TYPE.SESAME, "engineId", "engineName");

            ReadOnlyOWLEngine engine = factory.getReadOWL();
            assertEquals("engineId", engine.engineId);
            assertEquals("engineName", engine.engineName);
        }

    }

    @Test
    void testGetWriteOWL() throws InterruptedException {
        try (MockedStatic<Utility> utilityMockedStatic = Mockito.mockStatic(Utility.class)) {
            RDFFileSesameEngine rfse = mock(RDFFileSesameEngine.class);

            Vector<String> empty = new Vector<>();
            utilityMockedStatic.when(() -> Utility.getVectorOfReturn(anyString(), eq(rfse), eq(true))).thenReturn(empty);
            Vector<String[]> emptyArray = new Vector<>();
            utilityMockedStatic.when(() -> Utility.getVectorArrayOfReturn(anyString(), eq(rfse), eq(true))).thenReturn(emptyArray);

            factory = new OWLEngineFactory(rfse, IDatabaseEngine.DATABASE_TYPE.SESAME, "engineId", "engineName");

            WriteOWLEngine engine = factory.getWriteOWL();
            assertEquals("engineId", engine.engineId);
            assertEquals("engineName", engine.engineName);
        }
    }
}
