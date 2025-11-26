package prerna.engine.impl.rdf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import prerna.engine.api.IRDFDatabase;
import prerna.util.Utility;

public class RDFDefaultDatabaseTypeFactoryUnitTests {

    @Test
    void testGetDefaultSesameEngine() {
        try (MockedStatic<Utility> utilityMockedStatic = Mockito.mockStatic(Utility.class)) {
            utilityMockedStatic.when(() -> Utility.getDIHelperProperty(RDFDefaultDatabaseTypeFactory.DEFAULT_RDF_ENGINE))
                    .thenReturn("missing");

            IRDFDatabase sesame = RDFDefaultDatabaseTypeFactory.getDefaultRdfEngine();

            assertEquals("RDFJenaTDBEngine", sesame.getClass().getSimpleName());
        }
    }

}
