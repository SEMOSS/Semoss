package prerna.unit.auth.utils.reactors.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import java.util.Map;
import java.util.TreeSet;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityOwlCreator;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.auth.utils.reactors.admin.AdminGetProjectAvailableReactorsReactor;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.query.interpreters.sql.SqlInterpreter;
import prerna.rdf.engine.wrappers.RawRDBMSSelectWrapper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class AdminGetProjectAvailableReactorsReactorUnitTests {

    private AdminGetProjectAvailableReactorsReactor reactor;
    private Insight insight;
    private User user;

    private Map<String, String> keyValues;

    @BeforeEach
    void setup() {
        reactor = new AdminGetProjectAvailableReactorsReactor();
        insight = mock(Insight.class);
        user = mock(User.class);
        reactor.setInsight(insight);
        when(insight.getUser()).thenReturn(user);

        keyValues = reactor.keyValue;
    }

    private static void setupMocks(MockedStatic<Utility> util, MockedStatic<AbstractSecurityUtils> asu) throws Exception {
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

    @Test
    void testAdminUtilsNull() {
        try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
            sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(null);

            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
            assertEquals("User must be an admin to perform this function", e.getMessage());
        }
        //when(sau.apply(user)).thenReturn(null);
    }

    @Test
    void testProjectIdNull() throws Exception {
        try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
             MockedConstruction<SecurityOwlCreator> ignored = Mockito.mockConstruction(SecurityOwlCreator.class, (mock, context) -> {
                 when(mock.needsRemake()).thenReturn(false);
             });
             MockedStatic<AbstractSecurityUtils> asu = Mockito.mockStatic(AbstractSecurityUtils.class);
             MockedConstruction<RawRDBMSSelectWrapper> ignored1 = Mockito.mockConstruction(RawRDBMSSelectWrapper.class, (mock, context) -> {
                 when(mock.hasNext()).thenReturn(true);
             })) {

            setupMocks(util, asu);
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
            assertEquals("Must input an project id", e.getMessage());
        }
    }


    @Test
    void testProjectIdEmpty() {
        keyValues.put(ReactorKeysEnum.PROJECT.getKey(), "");
        try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
             MockedConstruction<SecurityOwlCreator> ignored = Mockito.mockConstruction(SecurityOwlCreator.class, (mock, context) -> {
                 when(mock.needsRemake()).thenReturn(false);
             });
             MockedStatic<AbstractSecurityUtils> asu = Mockito.mockStatic(AbstractSecurityUtils.class);
             MockedConstruction<RawRDBMSSelectWrapper> ignored1 = Mockito.mockConstruction(RawRDBMSSelectWrapper.class, (mock, context) -> {
                 when(mock.hasNext()).thenReturn(true);
             })) {

            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
            assertEquals("Must input an project id", e.getMessage());
        }
    }

    @Test
    void testExecuteNoReactorsReturned() throws Exception {
        String projectAlias = "test";
        String projectId = "testy";
        keyValues.put(ReactorKeysEnum.PROJECT.getKey(), projectAlias);
        try (MockedStatic<SecurityProjectUtils> spu = Mockito.mockStatic(SecurityProjectUtils.class);
             MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
             MockedConstruction<SecurityOwlCreator> ignored = Mockito.mockConstruction(SecurityOwlCreator.class, (mock, context) -> {
                 when(mock.needsRemake()).thenReturn(false);
             });
             MockedStatic<AbstractSecurityUtils> asu = Mockito.mockStatic(AbstractSecurityUtils.class);
             MockedConstruction<RawRDBMSSelectWrapper> ignored1 = Mockito.mockConstruction(RawRDBMSSelectWrapper.class, (mock, context) -> {
                 when(mock.hasNext()).thenReturn(true);
             })) {

            setupMocks(util, asu);

            spu.when(() -> SecurityProjectUtils.testUserProjectIdForAlias(user, projectAlias))
                    .thenReturn(projectId);

            IProject project = mock(IProject.class);
            util.when(() -> Utility.getProject(projectId)).thenReturn(project);

            TreeSet<String> emptyTreeSet = new TreeSet<>();

            when(project.getAvailableReactors()).thenReturn(emptyTreeSet);
            NounMetadata nm = reactor.execute();
            assertNotNull(nm.getValue());
            TreeSet<String> retValue = (TreeSet<String>) nm.getValue();
            assertEquals(emptyTreeSet.size(), retValue.size());
            assertEquals(PixelDataType.CONST_STRING, nm.getNounType());

            spu.verify(() -> SecurityProjectUtils.testUserProjectIdForAlias(user, projectAlias), times(1));
            util.verify(() -> Utility.getProject(projectId), times(1));
        }
    }

    @Test
    void testExecuteOneReactorReturned() throws Exception {
        String projectAlias = "test";
        String projectId = "testy";
        keyValues.put(ReactorKeysEnum.PROJECT.getKey(), projectAlias);
        try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
             MockedConstruction<SecurityOwlCreator> ignored = Mockito.mockConstruction(SecurityOwlCreator.class, (mock, context) -> {
                 when(mock.needsRemake()).thenReturn(false);
             });
             MockedStatic<AbstractSecurityUtils> asu = Mockito.mockStatic(AbstractSecurityUtils.class);
             MockedConstruction<RawRDBMSSelectWrapper> ignored1 = Mockito.mockConstruction(RawRDBMSSelectWrapper.class, (mock, context) -> {
                 when(mock.hasNext()).thenReturn(true);
             });
             MockedStatic<SecurityProjectUtils> spu = Mockito.mockStatic(SecurityProjectUtils.class)) {


            setupMocks(util, asu);

            spu.when(() -> SecurityProjectUtils.testUserProjectIdForAlias(user, projectAlias))
                    .thenReturn(projectId);

            IProject project = mock(IProject.class);
            util.when(() -> Utility.getProject(projectId)).thenReturn(project);

            TreeSet<String> emptyTreeSet = new TreeSet<>();
            emptyTreeSet.add("reactor 1");

            when(project.getAvailableReactors()).thenReturn(emptyTreeSet);
            NounMetadata nm = reactor.execute();
            assertNotNull(nm.getValue());
            TreeSet<String> retValue = (TreeSet<String>) nm.getValue();
            assertEquals(emptyTreeSet.size(), retValue.size());
            assertEquals("reactor 1", retValue.first());
            assertEquals(PixelDataType.CONST_STRING, nm.getNounType());

            spu.verify(() -> SecurityProjectUtils.testUserProjectIdForAlias(user, projectAlias), times(1));
            util.verify(() -> Utility.getProject(projectId), times(1));
        }
    }
}
