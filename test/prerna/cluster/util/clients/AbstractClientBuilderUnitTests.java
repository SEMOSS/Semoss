/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.cluster.util.clients;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AbstractClientBuilderUnitTests {

    private static class TestableClientBuilder extends AbstractClientBuilder {
        @Override
        public ICloudClient buildClient() {
            return null;
        }

        @Override
        public ICloudClientBuilder pullValuesFromSystem() {
            return this;
        }
    }

    private TestableClientBuilder builder;

    @BeforeEach
    void setUp() {
        builder = new TestableClientBuilder();
    }

    @Test
    void testDefaultRclonePathIsRclone() {
        assertEquals("rclone", builder.getRClonePath());
    }

    @Test
    void testDefaultConfigFolderIsNull() {
        assertNull(builder.getRCloneConfigFolder());
    }

    @Test
    void testSetRClonePathNoSpaces() {
        builder.setRClonePath("/usr/bin/rclone");
        assertEquals("/usr/bin/rclone", builder.getRClonePath());
    }

    @Test
    void testSetRClonePathSimpleName() {
        builder.setRClonePath("myRclone");
        assertEquals("myRclone", builder.getRClonePath());
    }

    @Test
    void testSetRClonePathWithSpacesAddsQuotes() {
        builder.setRClonePath("/program files/rclone");
        assertEquals("\"/program files/rclone\"", builder.getRClonePath());
    }

    @Test
    void testSetRClonePathWithSpacesInMiddle() {
        builder.setRClonePath("C:/my folder/bin/rclone");
        assertEquals("\"C:/my folder/bin/rclone\"", builder.getRClonePath());
    }

    @Test
    void testSetRClonePathAlreadyFullyQuoted() {
        builder.setRClonePath("\"/program files/rclone\"");
        assertEquals("\"/program files/rclone\"", builder.getRClonePath());
    }

    @Test
    void testSetRClonePathOnlyStartQuoteAddsEndQuote() {
        builder.setRClonePath("\"C:/my folder/rclone");
        assertEquals("\"C:/my folder/rclone\"", builder.getRClonePath());
    }

    @Test
    void testSetRClonePathOnlyEndQuoteAddsStartQuote() {
        builder.setRClonePath("C:/my folder/rclone\"");
        assertEquals("\"C:/my folder/rclone\"", builder.getRClonePath());
    }

    @Test
    void testSetRClonePathNullDoesNotChangeDefault() {
        builder.setRClonePath(null);
        assertEquals("rclone", builder.getRClonePath());
    }

    @Test
    void testSetRClonePathEmptyStringDoesNotChangeDefault() {
        builder.setRClonePath("");
        assertEquals("rclone", builder.getRClonePath());
    }

    @Test
    void testSetRClonePathNullAfterSetDoesNotChangeValue() {
        builder.setRClonePath("/usr/bin/rclone");
        builder.setRClonePath(null);
        assertEquals("/usr/bin/rclone", builder.getRClonePath());
    }

    @Test
    void testSetRClonePathEmptyAfterSetDoesNotChangeValue() {
        builder.setRClonePath("/usr/bin/rclone");
        builder.setRClonePath("");
        assertEquals("/usr/bin/rclone", builder.getRClonePath());
    }

    @Test
    void testSetRCloneConfigFolderSetsAndGets() {
        builder.setRCloneConfigFolder("/etc/rclone");
        assertEquals("/etc/rclone", builder.getRCloneConfigFolder());
    }

    @Test
    void testSetRCloneConfigFolderNull() {
        builder.setRCloneConfigFolder("/etc/rclone");
        builder.setRCloneConfigFolder(null);
        assertNull(builder.getRCloneConfigFolder());
    }

    @Test
    void testSetRCloneConfigFolderEmptyString() {
        builder.setRCloneConfigFolder("");
        assertEquals("", builder.getRCloneConfigFolder());
    }

    @Test
    void testSetRClonePathReturnsSelf() {
        ICloudClientBuilder result = builder.setRClonePath("/usr/bin/rclone");
        assertSame(builder, result);
    }

    @Test
    void testSetRCloneConfigFolderReturnsSelf() {
        ICloudClientBuilder result = builder.setRCloneConfigFolder("/etc/rclone");
        assertSame(builder, result);
    }

    @Test
    void testMethodChainingBothSetters() {
        ICloudClientBuilder result = builder
                .setRClonePath("/usr/bin/rclone")
                .setRCloneConfigFolder("/etc/rclone");
        assertSame(builder, result);
        assertEquals("/usr/bin/rclone", builder.getRClonePath());
        assertEquals("/etc/rclone", builder.getRCloneConfigFolder());
    }

    @Test
    void testRclonePathConstant() {
        assertEquals("RCLONE_PATH", AbstractClientBuilder.RCLONE_PATH);
    }

    @Test
    void testS3RegionKeyConstant() {
        assertEquals("S3_REGION", AbstractClientBuilder.S3_REGION_KEY);
    }

    @Test
    void testS3BucketKeyConstant() {
        assertEquals("S3_BUCKET", AbstractClientBuilder.S3_BUCKET_KEY);
    }

    @Test
    void testS3AccessKeyConstant() {
        assertEquals("S3_ACCESS_KEY", AbstractClientBuilder.S3_ACCESS_KEY);
    }

    @Test
    void testS3SecretKeyConstant() {
        assertEquals("S3_SECRET_KEY", AbstractClientBuilder.S3_SECRET_KEY);
    }

    @Test
    void testS3EndpointKeyConstant() {
        assertEquals("S3_ENDPOINT", AbstractClientBuilder.S3_ENDPOINT_KEY);
    }

    @Test
    void testGcpServiceAccountFileKeyConstant() {
        assertEquals("GCP_SERVICE_ACCOUNT_FILE", AbstractClientBuilder.GCP_SERVICE_ACCOUNT_FILE_KEY);
    }

    @Test
    void testGcpRegionKeyConstant() {
        assertEquals("GCP_REGION", AbstractClientBuilder.GCP_REGION_KEY);
    }

    @Test
    void testGcpBucketKeyConstant() {
        assertEquals("GCP_BUCKET", AbstractClientBuilder.GCP_BUCKET_KEY);
    }

    @Test
    void testAzConnStringConstant() {
        assertEquals("AZ_CONN_STRING", AbstractClientBuilder.AZ_CONN_STRING);
    }

    @Test
    void testAzNameConstant() {
        assertEquals("AZ_NAME", AbstractClientBuilder.AZ_NAME);
    }

    @Test
    void testAzKeyConstant() {
        assertEquals("AZ_KEY", AbstractClientBuilder.AZ_KEY);
    }

    @Test
    void testAzGenerateDynamicSasConstant() {
        assertEquals("AZ_GENERATE_DYNAMIC_SAS", AbstractClientBuilder.AZ_GENERATE_DYNAMIC_SAS);
    }

    @Test
    void testSasUrlConstant() {
        assertEquals("SAS_URL", AbstractClientBuilder.SAS_URL);
    }

    @Test
    void testAzUriConstant() {
        assertEquals("AZ_URI", AbstractClientBuilder.AZ_URI);
    }

    @Test
    void testStorageConstant() {
        assertEquals("STORAGE", AbstractClientBuilder.STORAGE);
    }

    @Test
    void testKeyHomeConstant() {
        assertEquals("KEY_HOME", AbstractClientBuilder.KEY_HOME);
    }

}
