package prerna.engine.impl.function;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.URLConnection;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.documentai.v1.BatchDocumentsInputConfig;
import com.google.cloud.documentai.v1.BatchProcessMetadata;
import com.google.cloud.documentai.v1.BatchProcessRequest;
import com.google.cloud.documentai.v1.BatchProcessResponse;
import com.google.cloud.documentai.v1.Document;
import com.google.cloud.documentai.v1.DocumentOutputConfig;
import com.google.cloud.documentai.v1.DocumentOutputConfig.GcsOutputConfig;
import com.google.cloud.documentai.v1.DocumentProcessorServiceClient;
import com.google.cloud.documentai.v1.DocumentProcessorServiceSettings;
import com.google.cloud.documentai.v1.GcsDocument;
import com.google.cloud.documentai.v1.GcsDocuments;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.protobuf.util.JsonFormat;

import prerna.engine.api.FunctionTypeEnum;
import prerna.engine.api.ICustomEmbeddingsFunctionEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IStorageEngine;
import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.reactor.export.pdf.PDFUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class GoogleOCRCustomEmbeddingsFunctionEngine extends AbstractFunctionEngine
    implements ICustomEmbeddingsFunctionEngine {

  private static final Logger classLogger =
      LogManager.getLogger(GoogleOCRCustomEmbeddingsFunctionEngine.class);

  private static final String DIR_SEPARATOR = "/";

  private static final String BUCKETENGINEID = "GOOGLE_BUCKET_ENGINEID";
  private static final String OUTPUTBUCKET = "gcp-service-repos";
  private static final String SMSS_KEY_SERVICE_ACCOUNT_CREDENTIALS = "SERVICE_ACCOUNT_CREDENTIALS";
  private static final String REGION = "REGION";
  private static final String PROJECT_ID = "PROJECT_ID";
  private static final String PROCESSOR_ID = "PROCESSOR_ID";
  private static final String SUB_FOLDER = "ocr/";
  private static final String BUCKET_PREFIX = "gs://";
  private static final String JSON_EXT = ".json";
  private static final String OUTPUT = "output";

  private String projectId;
  private String processorId;
  private String region;
  private String googleBucketEngineId;
  private String ServiceAccountFile;
  private Storage storage = null;
  private DocumentProcessorServiceClient client = null;

  @Override
  public void open(Properties smssProp) throws Exception {
    // preset these - don't need user to define
    smssProp.putIfAbsent(
        IFunctionEngine.NAME_KEY, "Google OCR - For Use With Vector Database Engines");
    smssProp.putIfAbsent(IFunctionEngine.DESCRIPTION_KEY, "Execute Google OCR");
    super.open(smssProp);

    this.projectId = smssProp.getProperty(PROJECT_ID);
    this.processorId = smssProp.getProperty(PROCESSOR_ID);
    this.region = smssProp.getProperty(REGION);
    this.googleBucketEngineId = smssProp.getProperty(BUCKETENGINEID);
    this.ServiceAccountFile = smssProp.getProperty(SMSS_KEY_SERVICE_ACCOUNT_CREDENTIALS);

    final String SCOPE_KEY = "https://www.googleapis.com/auth/cloud-platform";
    final String SCOPE_VALUE = "https://www.googleapis.com/auth/cloud-platform.read-only";
    final String ENDPOINT_FORMAT = "%s-documentai.googleapis.com:443";

    final String PROJECTID_ERRMSG = "Must pass in a project Id";
    final String PROCESSORID_ERRMSG = "Must pass in a processor Id";
    final String REGION_ERRMSG = "Must pass in a region";
    final String GOOGLEBUCKETENGINEID_ERRMSG = "Must pass in a google bucket EngineId";
    final String SERVICEACCFILE_ERRMSG = "Must pass in a Service Account File";

    if (this.projectId == null || this.projectId.isEmpty()) {
      throw new RuntimeException(PROJECTID_ERRMSG);
    }
    if (this.processorId == null || this.processorId.isEmpty()) {
      throw new RuntimeException(PROCESSORID_ERRMSG);
    }
    if (this.region == null || this.region.isEmpty()) {
      throw new RuntimeException(REGION_ERRMSG);
    }

    if (this.googleBucketEngineId == null || this.googleBucketEngineId.isEmpty()) {
      throw new RuntimeException(GOOGLEBUCKETENGINEID_ERRMSG);
    }

    if (this.ServiceAccountFile == null || this.ServiceAccountFile.isEmpty()) {
      throw new RuntimeException(SERVICEACCFILE_ERRMSG);
    }

    try {
      GoogleCredentials credentials =
          GoogleCredentials.fromStream(new ByteArrayInputStream(this.ServiceAccountFile.getBytes()))
              .createScoped(SCOPE_KEY, SCOPE_VALUE);

      FixedCredentialsProvider credentialsProvider = FixedCredentialsProvider.create(credentials);
      String endpoint = String.format(ENDPOINT_FORMAT, this.region);

      this.client =
          DocumentProcessorServiceClient.create(
              DocumentProcessorServiceSettings.newBuilder()
                  .setCredentialsProvider(credentialsProvider)
                  .setEndpoint(endpoint)
                  .build());
      this.storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();

    } catch (IOException e) {
      classLogger.error(Constants.STACKTRACE, e);
      throw e;
    }
  }

  @Override
  public Object execute(Map<String, Object> parameterValues) {
    throw new IllegalArgumentException(
        "This function engine is only intended to be executed for custom vector db embeddings");
  }

  @Override
  public boolean canProcessDocument(File fileToProcess) {
    boolean pdf = fileToProcess.getName().toLowerCase().endsWith(".pdf");
    if (pdf) {
      try {
        return PDFUtility.pdfContainsImages(fileToProcess.getAbsolutePath());
      } catch (IOException e) {
        classLogger.error(Constants.STACKTRACE, e);
      }
    }
    return false;
  }

  @Override
  public int processDocument(
      String outputCsvFilePath, File fileToProcess, Map<String, Object> parameters) {
    VectorDatabaseCSVWriter writer = new VectorDatabaseCSVWriter(outputCsvFilePath);
    List<String> extractedTextFromDoc = new ArrayList<String>();
    final String WAITING_INFO = "Waiting for operation to complete...";

    IStorageEngine storageeng = Utility.getStorage(this.googleBucketEngineId);
    Map<String, Object> metadata = new HashMap<>();
    metadata.put("utility", fileToProcess.getName() + "- GoogleOCR_functionality");

    try {
      storageeng.copyToStorage(
          fileToProcess.toString(),
          OUTPUTBUCKET + DIR_SEPARATOR + SUB_FOLDER + fileToProcess.getName(),
          metadata);

      classLogger.info(WAITING_INFO);
      String fileName = fileToProcess.getName();
      extractedTextFromDoc = getExtractedText(fileToProcess);
      for (int i = 0; i < extractedTextFromDoc.size(); i++) {
        writer.writeRow(fileName, String.valueOf(i + 1), extractedTextFromDoc.get(i));
      }
    } catch (Exception e) {
      classLogger.error(Constants.STACKTRACE, e);
    } finally {
      writer.close();
    }

    return writer.getRowsInCsv();
  }

  private List<String> getExtractedText(File fileToProcess) throws Exception {

    String filePathInBucket = null;
    List<String> extractedTextFromDoc = new ArrayList<String>();
    final String PROCESSORNAME_FORMAT = "projects/%s/locations/%s/processors/%s";
    final String END_INFO = "Document processing complete.";

    String processorName =
        String.format(PROCESSORNAME_FORMAT, this.projectId, this.region, this.processorId);

    filePathInBucket =
        BUCKET_PREFIX + OUTPUTBUCKET + DIR_SEPARATOR + SUB_FOLDER + fileToProcess.getName();

    // File inputFile = new File(filePathInBucket);
    try {
      String mimeType = URLConnection.guessContentTypeFromName(fileToProcess.getName());

      if (mimeType == null) {
        try (FileInputStream fis = new FileInputStream(fileToProcess)) {
          mimeType = URLConnection.guessContentTypeFromStream(fis);
        } catch (IOException e) {
          classLogger.error(Constants.STACKTRACE, e);
        }
      }

      GcsDocument gcsDocument =
          GcsDocument.newBuilder().setGcsUri(filePathInBucket).setMimeType(mimeType).build();

      GcsDocuments gcsDocuments = GcsDocuments.newBuilder().addDocuments(gcsDocument).build();

      BatchDocumentsInputConfig inputConfig =
          BatchDocumentsInputConfig.newBuilder().setGcsDocuments(gcsDocuments).build();

      LocalDateTime now = LocalDateTime.now();
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
      String formattedTimestamp = now.format(formatter);

      String outputFileName = fileToProcess.getName().concat("_" + formattedTimestamp + JSON_EXT);

      String fullGcsPath =
          String.format("gs://%s/%s/%s", OUTPUTBUCKET, SUB_FOLDER + OUTPUT, outputFileName);
      GcsOutputConfig gcsOutputConfig = GcsOutputConfig.newBuilder().setGcsUri(fullGcsPath).build();

      DocumentOutputConfig documentOutputConfig =
          DocumentOutputConfig.newBuilder().setGcsOutputConfig(gcsOutputConfig).build();

      // Configure the batch process request.
      BatchProcessRequest request =
          BatchProcessRequest.newBuilder()
              .setName(processorName)
              .setInputDocuments(inputConfig)
              .setDocumentOutputConfig(documentOutputConfig)
              .build();

      OperationFuture<BatchProcessResponse, BatchProcessMetadata> future =
          client.batchProcessDocumentsAsync(request);

      future.get();

      classLogger.info(END_INFO);

      extractedTextFromDoc = getTextFromStorage(outputFileName);

    } catch (Exception e) {
      classLogger.error(Constants.STACKTRACE, e);
      throw e;
    }
    return extractedTextFromDoc;
  }

  private List<String> getTextFromStorage(String outputFileName) throws IOException {
    List<String> extractedTextFromDoc = new ArrayList<String>();

    Bucket bucket = this.storage.get(OUTPUTBUCKET);

    Page<Blob> blobs =
        bucket.list(
            Storage.BlobListOption.prefix(SUB_FOLDER + OUTPUT + DIR_SEPARATOR + outputFileName));
    for (Blob blob : blobs.iterateAll()) {
      if (!blob.isDirectory()) {
        File tempFile = null;
        try {
          tempFile = File.createTempFile("file", JSON_EXT);
          Blob fileInfo = storage.get(BlobId.of(OUTPUTBUCKET, blob.getName()));
          fileInfo.downloadTo(tempFile.toPath());
          try (FileReader reader = new FileReader(tempFile)) {
            Document.Builder builder = Document.newBuilder();
            JsonFormat.parser().merge(reader, builder);
            Document document = builder.build();
            for (int pageIndex = 0; pageIndex < document.getPagesCount(); pageIndex++) {
              Document.Page page = document.getPages(pageIndex); // Get the current page
              String pageText = getText(page.getLayout().getTextAnchor(), document.getText());
              extractedTextFromDoc.add(pageText); // Store page-wise text
            }
          }
        } catch (IOException e) {
          classLogger.error(Constants.STACKTRACE, e);
          throw e;
        } finally {
          if (tempFile != null && tempFile.exists()) {
            tempFile.delete();
          }
        }
      }
    }

    return extractedTextFromDoc;
  }

  private static String getText(Document.TextAnchor textAnchor, String text) {
    if (textAnchor.getTextSegmentsList().size() > 0) {
      int startIdx = (int) textAnchor.getTextSegments(0).getStartIndex();
      int endIdx = (int) textAnchor.getTextSegments(0).getEndIndex();
      return text.substring(startIdx, endIdx);
    }
    return " ";
  }

  @Override
  public void close() throws IOException {
    if (this.client != null) {
      this.client.close();
    }
    if (this.storage != null) {
      try {
        this.storage.close();
      } catch (Exception e) {
        classLogger.error(Constants.STACKTRACE, e);
      }
    }
  }

  @Override
  public String getCatalogSubType(Properties smssProp) {
    return FunctionTypeEnum.GOOGLE_OCR_CUSTOM_EMBEDDINGS.name();
  }
}
