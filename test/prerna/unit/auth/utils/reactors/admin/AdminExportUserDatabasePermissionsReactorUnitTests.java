package prerna.unit.auth.utils.reactors.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

import org.apache.poi.ss.usermodel.Workbook;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.reactors.admin.AdminExportUserDatabasePermissionsReactor;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.Insight;
import prerna.om.InsightPanel;
import prerna.om.InsightSheet;
import prerna.poi.main.helper.excel.ExcelUtility;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class AdminExportUserDatabasePermissionsReactorUnitTests {

	private FileSystem fs = Jimfs.newFileSystem(Configuration.unix());

	private AdminExportUserDatabasePermissionsReactor reactor;
	private Insight insight;
	private User user;
	private NounStore ns;

	private GenRowStruct fnGrs;
	private GenRowStruct fpGrs;
	private GenRowStruct pwGrs;
	private GenRowStruct dbGrs;
	private GenRowStruct panelGrs;

	@BeforeEach
	void setup() throws IOException {
		reactor = new AdminExportUserDatabasePermissionsReactor();
		reactor.setFileSystem(fs);
		insight = mock(Insight.class);
		user = mock(User.class);
		reactor.setInsight(insight);
		when(insight.getUser()).thenReturn(user);

		ns = mock(NounStore.class);

		fnGrs = mock(GenRowStruct.class);
		fpGrs = mock(GenRowStruct.class);
		pwGrs = mock(GenRowStruct.class);
		dbGrs = mock(GenRowStruct.class);
		panelGrs = mock(GenRowStruct.class);

		reactor.setNounStore(ns);

		Path p = fs.getPath("work", "insight1");
		Files.createDirectories(p);

	}

	@Test
	void testAdminUtilsNull() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(null);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("User must be an admin to perform this function", e.getMessage());
		}
		// when(sau.apply(user)).thenReturn(null);
	}

	@Test
	void testPasswordIsNull() {
		when(ns.getNoun(ReactorKeysEnum.FILE_NAME.getKey())).thenReturn(null);
		when(ns.getNoun(ReactorKeysEnum.FILE_PATH.getKey())).thenReturn(null);
		when(ns.getNoun(ReactorKeysEnum.PASSWORD.getKey())).thenReturn(null);
		when(ns.getNoun(ReactorKeysEnum.DATABASE.getKey())).thenReturn(null);

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Must provide a password to encrypt the file", e.getMessage());
		}
	}

	@Test
	void testPasswordIsEmpty() {
		when(ns.size()).thenReturn(2);

		when(ns.getNoun(ReactorKeysEnum.PASSWORD.getKey())).thenReturn(pwGrs);

		when(pwGrs.isEmpty()).thenReturn(false);
		when(pwGrs.get(0)).thenReturn("");

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Must provide a password to encrypt the file", e.getMessage());
		}
	}

	@Test
	void testFileEmpty() throws Exception {
		when(ns.size()).thenReturn(2);

		when(ns.getNoun(ReactorKeysEnum.PASSWORD.getKey())).thenReturn(pwGrs);

		when(ns.getNoun(ReactorKeysEnum.PANEL.getKey())).thenReturn(panelGrs);
		when(panelGrs.isEmpty()).thenReturn(false);

		InsightPanel ip = mock(InsightPanel.class);
		NounMetadata nm = new NounMetadata(ip, PixelDataType.PANEL);
		Map<String, Map<String, String>> panelFormatting = new HashMap<>();
		when(ip.getPanelFormatValues()).thenReturn(panelFormatting);
		when(ip.getSheetId()).thenReturn("0");

		InsightSheet is = mock(InsightSheet.class);
		when(insight.getInsightSheet("0")).thenReturn(is);
		when(is.getSheetLabel()).thenReturn("sheetLabel");

		when(panelGrs.getNoun(0)).thenReturn(nm);

		when(pwGrs.isEmpty()).thenReturn(false);
		when(pwGrs.get(0)).thenReturn("encrypt");

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class);
				MockedStatic<WrapperManager> wm = Mockito.mockStatic(WrapperManager.class);
				MockedStatic<ExcelUtility> eu = Mockito.mockStatic(ExcelUtility.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			RDBMSNativeEngine db = mock(RDBMSNativeEngine.class);
			utility.when(() -> Utility.getDatabase(Constants.SECURITY_DB)).thenReturn(db);

			WrapperManager wmmock = mock(WrapperManager.class);
			wm.when(WrapperManager::getInstance).thenReturn(wmmock);

			IRawSelectWrapper irswMock = mock(IRawSelectWrapper.class);
			when(wmmock.getRawWrapper(eq(db), any(SelectQueryStruct.class))).thenReturn(irswMock);

			ZonedDateTime zdt = ZonedDateTime.of(LocalDateTime.of(2025, 2, 14, 1, 1), ZoneId.of("UTC"));
			utility.when(() -> Utility.getCurrentZonedDateTimeForUser(user)).thenReturn(zdt);

			utility.when(() -> Utility.normalizePath("/work/insight1")).thenReturn("/work/insight1");
			utility.when(() -> Utility.normalizePath("/work/insight1/null.xlsx"))
					.thenReturn("/work/insight1/null.xlsx");

			Path p = fs.getPath("insight1");
			when(insight.getInsightFolder()).thenReturn(p.toAbsolutePath().toString());

			when(irswMock.hasNext()).thenReturn(false);

			NounMetadata result = reactor.execute();
			assertNotNull(result.getValue());
			assertEquals(PixelDataType.CONST_STRING, result.getNounType());
			assertEquals(PixelOperationType.FILE_DOWNLOAD, result.getOpType().get(0));
			NounMetadata additionalResult = result.getAdditionalReturn().get(0);
			assertEquals("Successfully generated the excel file", additionalResult.getValue().toString());
			assertEquals(PixelDataType.CONST_STRING, additionalResult.getNounType());
			assertEquals(PixelOperationType.SUCCESS, additionalResult.getOpType().get(0));

			eu.verify(() -> ExcelUtility.encrypt(any(Workbook.class), eq("/work/insight1/null.xlsx"), eq("encrypt")),
					times(1));
		}
	}
}
