package prerna.ds.export.graph;

import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.awt.Color;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.ds.TinkerFrame;
import prerna.ds.rdbms.h2.H2Frame;

public class GraphExporterFactoryUnitTests {
	
    @Test
    void testGetExporter() {
    	// test valid tinker frame
        TinkerFrame frame = mock(TinkerFrame.class);
        IGraphExporter exporter = GraphExporterFactory.getExporter(frame);
        assertNotNull(exporter);
        assertTrue(exporter instanceof TinkerFrameGraphExporter);
        
        // test h2Frame
        H2Frame h2Frame = mock(H2Frame.class);
        assertNull(GraphExporterFactory.getExporter(h2Frame));
        
        // null test
        assertNull(GraphExporterFactory.getExporter(null));
    }
    
    @Test
    void testGetExporterColorMap() {
    	// create tinker frame and color map
        TinkerFrame frame = mock(TinkerFrame.class);
        Map<String, Color> colorMap = new HashMap<>();
        colorMap.put("person", Color.RED);

        IGraphExporter exporter = GraphExporterFactory.getExporter(frame, colorMap);
        assertTrue(exporter instanceof TinkerFrameGraphExporter);
        
    	// create h2 frame and color map
        H2Frame h2Frame = mock(H2Frame.class);
        assertNull(GraphExporterFactory.getExporter(h2Frame, colorMap));

    	// null
        assertNull(GraphExporterFactory.getExporter(null, colorMap));

    }
}
