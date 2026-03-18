package prerna.poi.main.helper.excel;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;

import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.stubbing.Answer;

import com.github.pjfanning.xlsx.StreamingReader;

import prerna.query.querystruct.ExcelQueryStruct;

class ExcelWorkbookFileHelperUnitTests {

    @TempDir
    Path tempDir;

    private Path createSimpleXlsx(String fileName, String sheetName) throws Exception {
        Path file = tempDir.resolve(fileName);
        try (XSSFWorkbook wb = new XSSFWorkbook()) {
            wb.createSheet(sheetName).createRow(0).createCell(0).setCellValue("v");
            try (FileOutputStream out = new FileOutputStream(file.toFile())) {
                wb.write(out);
            }
        }
        return file;
    }

    @Test
    void test_parseAndGetFilePathAndSheetsAndGetSheet() throws Exception {
        Path xlsx = createSimpleXlsx("book.xlsx", "Sheet1");

        ExcelWorkbookFileHelper helper = new ExcelWorkbookFileHelper();
        helper.parse(xlsx.toString(), null);

        assertEquals(xlsx.toString(), helper.getFilePath());

        List<String> sheets = helper.getSheets();
        assertNotNull(sheets);
        assertTrue(sheets.contains("Sheet1"));

        Sheet sheet = helper.getSheet("Sheet1");
        assertNotNull(sheet);

        helper.clear();
    }

    @Test
    @SuppressWarnings("deprecation")
    void test_parseDeprecatedOverloadWorks() throws Exception {
        Path xlsx = createSimpleXlsx("book_depr.xlsx", "Sheet1");

        ExcelWorkbookFileHelper helper = new ExcelWorkbookFileHelper();
        helper.parse(xlsx.toString());

        assertTrue(helper.getSheets().contains("Sheet1"));
        helper.clear();
    }

    @Test
    void test_parseNonexistentFileThrowsRuntimeException() {
        ExcelWorkbookFileHelper helper = new ExcelWorkbookFileHelper();

        RuntimeException ex = assertThrows(RuntimeException.class, () -> helper.parse("does-not-exist.xlsx", null));
        assertEquals("Excel file not found", ex.getMessage());
    }

    @Test
    void test_clearBeforeParseDoesNotThrow() {
        ExcelWorkbookFileHelper helper = new ExcelWorkbookFileHelper();
        assertDoesNotThrow(helper::clear);
    }

    @Test
    void test_clearAfterParseIsIdempotent() throws Exception {
        Path xlsx = createSimpleXlsx("book_clear.xlsx", "Sheet1");

        ExcelWorkbookFileHelper helper = new ExcelWorkbookFileHelper();
        helper.parse(xlsx.toString(), null);

        assertDoesNotThrow(helper::clear);
        assertDoesNotThrow(helper::clear);
    }

    @Test
    void test_getSheetIteratorReturnsIterator() throws Exception {
        Path xlsx = createSimpleXlsx("book_iter.xlsx", "Sheet1");

        ExcelWorkbookFileHelper helper = new ExcelWorkbookFileHelper();
        helper.parse(xlsx.toString(), null);

        ExcelQueryStruct qs = new ExcelQueryStruct();
        qs.setFilePath(xlsx.toString());
        qs.setSheetName("Sheet1");
        qs.setSheetRange("A1:A1");

        ExcelSheetFileIterator it = helper.getSheetIterator(qs);
        assertNotNull(it);

        helper.clear();
    }

    @Test
    void test_buildSheetIteratorReturnsIterator() throws Exception {
        Path xlsx = createSimpleXlsx("book_build.xlsx", "Sheet1");

        ExcelQueryStruct qs = new ExcelQueryStruct();
        qs.setFilePath(xlsx.toString());
        qs.setSheetName("Sheet1");
        qs.setSheetRange("A1:A1");
        qs.setPassword(null);

        ExcelSheetFileIterator it = ExcelWorkbookFileHelper.buildSheetIterator(qs);
        assertNotNull(it);
    }

    @Test
    void test_parseEncryptedDocumentExceptionWrapsWithHelpfulMessage() throws Exception {
        Path anyFile = tempDir.resolve("not_really_encrypted.xlsx");
        assertTrue(anyFile.toFile().createNewFile());

        StreamingReader.Builder builder = mock(StreamingReader.Builder.class, (Answer<Object>) invocation -> builder);
        when(builder.open(any(InputStream.class))).thenThrow(new EncryptedDocumentException("enc"));

        try (MockedStatic<StreamingReader> mocked = mockStatic(StreamingReader.class)) {
            mocked.when(StreamingReader::builder).thenReturn(builder);

            ExcelWorkbookFileHelper helper = new ExcelWorkbookFileHelper();
            RuntimeException ex = assertThrows(RuntimeException.class, () -> helper.parse(anyFile.toString(), "bad"));

            assertEquals("Unable to open encrypted Excel file. Please verify the password.", ex.getMessage());
        }
    }

    @Test
    void test_parseGenericOpenExceptionWrapsWithUnableToReadExcelFile() throws Exception {
        Path anyFile = tempDir.resolve("boom.xlsx");
        assertTrue(anyFile.toFile().createNewFile());

        StreamingReader.Builder builder = mock(StreamingReader.Builder.class, (Answer<Object>) invocation -> builder);
        when(builder.open(any(InputStream.class))).thenThrow(new RuntimeException("boom"));

        try (MockedStatic<StreamingReader> mocked = mockStatic(StreamingReader.class)) {
            mocked.when(StreamingReader::builder).thenReturn(builder);

            ExcelWorkbookFileHelper helper = new ExcelWorkbookFileHelper();
            RuntimeException ex = assertThrows(RuntimeException.class, () -> helper.parse(anyFile.toString(), null));

            assertEquals("Unable to read Excel file", ex.getMessage());
        }
    }
}
