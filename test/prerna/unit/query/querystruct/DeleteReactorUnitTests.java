package prerna.unit.query.querystruct;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.delete.DeleteReactor;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.ReactorKeysEnum;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class DeleteReactorUnitTests {
    private DeleteReactor reactor;

    @Mock
    private NounStore mockStore;

    @Mock
    private GenRowStruct mockTabGrs;

    @Mock
    private SelectQueryStruct selectQueryStruct;


    @BeforeEach
    void setup() {
    	MockitoAnnotations.openMocks(this);
		reactor = new DeleteReactor();
        reactor.setNounStore(mockStore);
        when(mockStore.getNoun(ReactorKeysEnum.COLUMNS.getKey())).thenReturn(mockTabGrs);

    }
    @Test
    public void testCreateQueryStruct() {
        // Setup
        mockTabGrs = mock(GenRowStruct.class);
        when(mockStore.getNoun("from")).thenReturn(mockTabGrs);
        when(mockTabGrs.get(0)).thenReturn("columnName");

        // Execute
        AbstractQueryStruct result = reactor.createQueryStruct();

        // Verify
        assertNotNull(result);
        assertTrue(result instanceof SelectQueryStruct);
        SelectQueryStruct qs = (SelectQueryStruct) result;
        List<IQuerySelector> selectors = qs.getSelectors();
        assertNotNull(selectors);
        assertTrue(selectors.get(0) instanceof QueryColumnSelector);

    }


    @Test
    public void testSetQs() {
        // Execute
        reactor.setQs(selectQueryStruct);
    }

    @Test
    public void testGetName() {
        // Execute
        String name = reactor.getName();
        assertEquals("Delete", name);
    }
}
