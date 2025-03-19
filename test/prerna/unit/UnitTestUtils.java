package prerna.unit;

import org.mockito.MockedStatic;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.interpreters.sql.SqlInterpreter;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UnitTestUtils {

    public static void setupMocks(MockedStatic<Utility> util, MockedStatic<AbstractSecurityUtils> asu) throws Exception {
        RDBMSNativeEngine dbEngine = mock(RDBMSNativeEngine.class);
        when(dbEngine.getEngineId()).thenReturn(Constants.SECURITY_DB);
        when(dbEngine.getDatabaseType()).thenReturn(IDatabaseEngine.DATABASE_TYPE.RDBMS);

        SqlInterpreter si = mock(SqlInterpreter.class);
        when(dbEngine.getQueryInterpreter()).thenReturn(si);
        when(si.composeQuery()).thenReturn("TEST QUERY");
        AbstractSqlQueryUtil queryUtil = mock(AbstractSqlQueryUtil.class);
        when(dbEngine.getQueryUtil()).thenReturn(queryUtil);
        util.when(() -> Utility.getDatabase(Constants.SECURITY_DB)).thenReturn(dbEngine);

        asu.when(() -> AbstractSecurityUtils.loadSecurityDatabase()).thenCallRealMethod();
        AbstractSecurityUtils.loadSecurityDatabase();
    }

}
