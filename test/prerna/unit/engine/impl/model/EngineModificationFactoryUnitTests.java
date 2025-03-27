package prerna.unit.engine.impl.model;



import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import prerna.engine.impl.modifications.EngineModificationFactory;
import prerna.engine.impl.modifications.RdbmsModifier;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IDatabaseEngine.DATABASE_TYPE;
import prerna.engine.api.IEngineModifier;

public class EngineModificationFactoryUnitTests {
    private IDatabaseEngine dbEngine;

    @BeforeEach
    void setUp() {
        dbEngine = mock(IDatabaseEngine.class);

    }

    @Test
    void testGetEngineModifierNull() {
        IEngineModifier testing = EngineModificationFactory.getEngineModifier(dbEngine); // class.method for every test for static calls
        assertNull(testing);
    }

    @Test
    void testGetEngineModifierRdbms() {
        try(MockedConstruction<RdbmsModifier> rmc = Mockito.mockConstruction(RdbmsModifier.class, (mock, context) -> {
            //do nothing
        })) {
            when(dbEngine.getDatabaseType()).thenReturn(DATABASE_TYPE.RDBMS);
            IEngineModifier testing = EngineModificationFactory.getEngineModifier(dbEngine); // class.method for every test for static calls
            assertNotNull(testing);
            RdbmsModifier red = (RdbmsModifier) testing;
            assertNotNull(red);
        }

    }

}
