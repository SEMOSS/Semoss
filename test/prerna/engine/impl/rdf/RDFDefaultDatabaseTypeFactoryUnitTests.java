package prerna.engine.impl.rdf;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import prerna.engine.api.ISesameRdfEngine;
import prerna.util.Utility;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RDFDefaultDatabaseTypeFactoryUnitTests {

    @Test
    void testGetDefaultSesameEngine() {
        try (MockedStatic<Utility> utilityMockedStatic = Mockito.mockStatic(Utility.class)) {
            utilityMockedStatic.when(() -> Utility.getDIHelperProperty(RDFDefaultDatabaseTypeFactory.DEFAULT_RDF_ENGINE))
                    .thenReturn("missing");

            ISesameRdfEngine sesame = RDFDefaultDatabaseTypeFactory.getDefaultSesameEngine();

            assertEquals("RDFFileSesameEngine", sesame.getClass().getSimpleName());
        }
    }

}
