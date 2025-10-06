package prerna.engine.impl.modifications;

import org.junit.jupiter.api.Test;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngineModifier;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdf.RemoteJenaEngine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EngineModificationFactoryUnitTests {


    @Test
    void testRDBMS() {
        IDatabaseEngine engine = mock(RDBMSNativeEngine.class);
        when(engine.getDatabaseType()).thenReturn(IDatabaseEngine.DATABASE_TYPE.RDBMS);

        IEngineModifier modifier = EngineModificationFactory.getEngineModifier(engine);

        assertEquals(RdbmsModifier.class, modifier.getClass());
    }


    @Test
    void testNonRDBMS() {
        IDatabaseEngine engine = new RemoteJenaEngine();
        IEngineModifier modifier = EngineModificationFactory.getEngineModifier(engine);
        assertNull(modifier);
    }
}
