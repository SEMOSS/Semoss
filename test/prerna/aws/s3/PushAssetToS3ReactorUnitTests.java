/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.aws.s3;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import prerna.om.Insight;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;

// Leaving in for now as an example for other devs to write unit tests for
@Deprecated
public class PushAssetToS3ReactorUnitTests {

  private PushAssetToS3Reactor reactor;
  private Map<String, String> keyValues;
  private Insight insight;

  @BeforeEach
  public void setUp() {
    reactor = new PushAssetToS3Reactor();
    keyValues = reactor.keyValue;

    insight = mock(Insight.class);
    reactor.setInsight(insight);
  }

  @Test
  public void testDescriptionForKeyBucket() {
    assertEquals("S3 bucket name", reactor.getDescriptionForKey("bucket"));
  }

  @Test
  public void testDescriptionForKeyNotBucket() {
    assertEquals("Base service endpoint", reactor.getDescriptionForKey("endpoint"));
  }

  @Test
  public void testDescriptionForKeyCommon() {
    assertEquals("The version", reactor.getDescriptionForKey("version"));
  }

  @Test
  public void testReactorDescription() {
    assertEquals(
        "Upload an asset file to an S3 bucket. Credentials can be set via a profile path/name or with an explicit access key and secret",
        reactor.getReactorDescription());
  }

  @Test
  public void testExecuteAssetDoesNOTExist() {
    keyValues.put(ReactorKeysEnum.FILE_PATH.getKey(), "fp");
    keyValues.put(ReactorKeysEnum.SPACE.getKey(), "space");
    keyValues.put("bucket", "bucket");
    try (MockedStatic<AssetUtility> au = mockStatic(AssetUtility.class);
        MockedStatic<S3Utils> s3 = mockStatic(S3Utils.class);
        MockedStatic<Files> fs = mockStatic(Files.class);
        MockedStatic<Paths> ps = mockStatic(Paths.class)) {

      au.when(() -> AssetUtility.getRootFolderPath(insight, "space", true)).thenReturn("path");

      AmazonS3 s3Client = mock(AmazonS3.class);
      S3Utils s3Utils = mock(S3Utils.class);
      s3.when(S3Utils::getInstance).thenReturn(s3Utils);

      when(s3Utils.getS3Client(keyValues)).thenReturn(s3Client);

      Path p = mock(Path.class);
      ps.when(() -> Paths.get("path/fp")).thenReturn(p);
      fs.when(() -> Files.exists(p)).thenReturn(false);

      SemossPixelException e = assertThrows(SemossPixelException.class, reactor::execute);
      assertEquals("File does not exist", e.getNoun().getValue());
    }
  }

  @Test
  public void testExecuteAssetIsDirectory() {
    keyValues.put(ReactorKeysEnum.FILE_PATH.getKey(), "fp");
    keyValues.put(ReactorKeysEnum.SPACE.getKey(), "space");
    keyValues.put("bucket", "bucket");
    try (MockedStatic<AssetUtility> au = mockStatic(AssetUtility.class);
        MockedStatic<S3Utils> s3 = mockStatic(S3Utils.class);
        MockedStatic<Files> fs = mockStatic(Files.class);
        MockedStatic<Paths> ps = mockStatic(Paths.class);
        MockedStatic<TransferManagerBuilder> tmBuilder = mockStatic(TransferManagerBuilder.class)) {

      au.when(() -> AssetUtility.getRootFolderPath(insight, "space", true)).thenReturn("path");

      AmazonS3 s3Client = mock(AmazonS3.class);
      S3Utils s3Utils = mock(S3Utils.class);
      s3.when(S3Utils::getInstance).thenReturn(s3Utils);

      when(s3Utils.getS3Client(keyValues)).thenReturn(s3Client);

      Path p = mock(Path.class);
      ps.when(() -> Paths.get("path/fp")).thenReturn(p);
      fs.when(() -> Files.exists(p)).thenReturn(true);

      TransferManagerBuilder tmb = mock(TransferManagerBuilder.class);
      tmBuilder.when(TransferManagerBuilder::standard).thenReturn(tmb);
      when(tmb.withS3Client(s3Client)).thenReturn(tmb);
      TransferManager tm = mock(TransferManager.class);
      when(tmb.build()).thenReturn(tm);

      fs.when(() -> Files.isDirectory(p)).thenReturn(true);

      MultipleFileUpload multiFileUpload = mock(MultipleFileUpload.class);

      when(tm.uploadDirectory("bucket", "fp", null, true)).thenReturn(multiFileUpload);

      NounMetadata nm = reactor.execute();
      assertTrue(Boolean.parseBoolean(nm.getValue().toString()));
      assertEquals(PixelDataType.BOOLEAN, nm.getNounType());
      assertEquals(PixelOperationType.SUCCESS, nm.getOpType().get(0));

      // verify upload directory
      verify(tm, times(1)).uploadDirectory("bucket", "fp", null, true);
      // verify no file upload
      verify(tm, times(0)).upload(any(), any(), any());
    }
  }

  @Test
  public void testExecuteAssetIsFile() {
    keyValues.put(ReactorKeysEnum.FILE_PATH.getKey(), "fp");
    keyValues.put(ReactorKeysEnum.SPACE.getKey(), "space");
    keyValues.put("bucket", "bucket");
    try (MockedStatic<AssetUtility> au = mockStatic(AssetUtility.class);
        MockedStatic<S3Utils> s3 = mockStatic(S3Utils.class);
        MockedStatic<Files> fs = mockStatic(Files.class);
        MockedStatic<Paths> ps = mockStatic(Paths.class);
        MockedStatic<TransferManagerBuilder> tmBuilder = mockStatic(TransferManagerBuilder.class)) {

      au.when(() -> AssetUtility.getRootFolderPath(insight, "space", true)).thenReturn("path");

      AmazonS3 s3Client = mock(AmazonS3.class);
      S3Utils s3Utils = mock(S3Utils.class);
      s3.when(S3Utils::getInstance).thenReturn(s3Utils);

      when(s3Utils.getS3Client(keyValues)).thenReturn(s3Client);

      Path p = mock(Path.class);
      ps.when(() -> Paths.get("path/fp")).thenReturn(p);
      fs.when(() -> Files.exists(p)).thenReturn(true);

      TransferManagerBuilder tmb = mock(TransferManagerBuilder.class);
      tmBuilder.when(TransferManagerBuilder::standard).thenReturn(tmb);
      when(tmb.withS3Client(s3Client)).thenReturn(tmb);
      TransferManager tm = mock(TransferManager.class);
      when(tmb.build()).thenReturn(tm);

      fs.when(() -> Files.isDirectory(p)).thenReturn(false);

      Upload upload = mock(Upload.class);

      when(tm.upload("bucket", "fp", null)).thenReturn(upload);

      NounMetadata nm = reactor.execute();
      assertTrue(Boolean.parseBoolean(nm.getValue().toString()));
      assertEquals(PixelDataType.BOOLEAN, nm.getNounType());
      assertEquals(PixelOperationType.SUCCESS, nm.getOpType().get(0));

      // verify no upload directory
      verify(tm, times(0)).uploadDirectory(any(), any(), any(), anyBoolean());
      // verify file upload
      verify(tm, times(1)).upload("bucket", "fp", null);
    }
  }

  @Test
  public void testExecuteException() {
    keyValues.put(ReactorKeysEnum.FILE_PATH.getKey(), "fp");
    keyValues.put(ReactorKeysEnum.SPACE.getKey(), "space");
    keyValues.put("bucket", "bucket");
    try (MockedStatic<AssetUtility> au = mockStatic(AssetUtility.class);
        MockedStatic<S3Utils> s3 = mockStatic(S3Utils.class);
        MockedStatic<Files> fs = mockStatic(Files.class);
        MockedStatic<Paths> ps = mockStatic(Paths.class);
        MockedStatic<TransferManagerBuilder> tmBuilder = mockStatic(TransferManagerBuilder.class)) {

      au.when(() -> AssetUtility.getRootFolderPath(insight, "space", true)).thenReturn("path");

      AmazonS3 s3Client = mock(AmazonS3.class);
      S3Utils s3Utils = mock(S3Utils.class);
      s3.when(S3Utils::getInstance).thenReturn(s3Utils);

      when(s3Utils.getS3Client(keyValues)).thenReturn(s3Client);

      Path p = mock(Path.class);
      ps.when(() -> Paths.get("path/fp")).thenReturn(p);
      fs.when(() -> Files.exists(p)).thenReturn(true);

      TransferManagerBuilder tmb = mock(TransferManagerBuilder.class);
      tmBuilder.when(TransferManagerBuilder::standard).thenReturn(tmb);
      when(tmb.withS3Client(s3Client)).thenReturn(tmb);
      TransferManager tm = mock(TransferManager.class);
      when(tmb.build()).thenReturn(tm);

      fs.when(() -> Files.isDirectory(p)).thenReturn(false);

      AmazonClientException ace = new AmazonClientException("err");
      when(tm.upload("bucket", "fp", null)).thenThrow(ace);

      NounMetadata nm = reactor.execute();
      assertEquals("Error occurred during upload", nm.getValue().toString());
      assertEquals(PixelDataType.ERROR, nm.getNounType());
      assertEquals(PixelOperationType.ERROR, nm.getOpType().get(0));
    }
  }

  @Test
  public void testExecuteFilePathNull() {
    keyValues.put(ReactorKeysEnum.SPACE.getKey(), "space");
    keyValues.put("bucket", "bucket");
    try (MockedStatic<AssetUtility> au = mockStatic(AssetUtility.class);
        MockedStatic<S3Utils> s3 = mockStatic(S3Utils.class)) {

      au.when(() -> AssetUtility.getRootFolderPath(insight, "space", true)).thenReturn("path");

      AmazonS3 s3Client = mock(AmazonS3.class);
      S3Utils s3Utils = mock(S3Utils.class);
      s3.when(S3Utils::getInstance).thenReturn(s3Utils);

      when(s3Utils.getS3Client(keyValues)).thenReturn(s3Client);

      NounMetadata nm = reactor.execute();
      assertTrue(Boolean.parseBoolean(nm.getValue().toString()));
      assertEquals(PixelDataType.BOOLEAN, nm.getNounType());
      assertEquals(PixelOperationType.SUCCESS, nm.getOpType().get(0));
    }
  }

  @Test
  public void testExecuteFilePathEmpty() {
    keyValues.put(ReactorKeysEnum.FILE_PATH.getKey(), "");
    keyValues.put(ReactorKeysEnum.SPACE.getKey(), "space");
    keyValues.put("bucket", "bucket");
    try (MockedStatic<AssetUtility> au = mockStatic(AssetUtility.class);
        MockedStatic<S3Utils> s3 = mockStatic(S3Utils.class)) {

      au.when(() -> AssetUtility.getRootFolderPath(insight, "space", true)).thenReturn("path");

      AmazonS3 s3Client = mock(AmazonS3.class);
      S3Utils s3Utils = mock(S3Utils.class);
      s3.when(S3Utils::getInstance).thenReturn(s3Utils);

      when(s3Utils.getS3Client(keyValues)).thenReturn(s3Client);

      NounMetadata nm = reactor.execute();
      assertTrue(Boolean.parseBoolean(nm.getValue().toString()));
      assertEquals(PixelDataType.BOOLEAN, nm.getNounType());
      assertEquals(PixelOperationType.SUCCESS, nm.getOpType().get(0));
    }
  }
}
