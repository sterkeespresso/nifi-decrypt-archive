/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.sterkeespresso.nifi.processors;

import static nl.sterkeespresso.nifi.processors.DecryptArchive.FRAGMENT_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Before;
import org.junit.Test;

public class DecryptArchiveTest {

    private static final Path dataPath = Paths.get("src/test/resources/DecryptArchiveTest");

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(DecryptArchive.class);
    }

    @Test
    public void testUnpackZip() throws IOException {
        testRunner.setProperty(DecryptArchive.MODE, DecryptArchive.DECRYPT_UNPACK_MODE);
        testRunner.setProperty(DecryptArchive.PASSWORD, "SOMEPASSWORD");
        testRunner.enqueue(dataPath.resolve("protected_data.zip"));
        testRunner.enqueue(dataPath.resolve("protected_data.zip"));
        testRunner.run(2);

        testRunner.assertTransferCount(DecryptArchive.REL_SUCCESS, 4);
        testRunner.assertTransferCount(DecryptArchive.REL_ORIGINAL, 2);
        testRunner.getFlowFilesForRelationship(DecryptArchive.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "2");
        testRunner.getFlowFilesForRelationship(DecryptArchive.REL_ORIGINAL).get(1).assertAttributeEquals(FRAGMENT_COUNT, "2");
        testRunner.assertTransferCount(DecryptArchive.REL_FAILURE, 0);

        final List<MockFlowFile> unpacked = testRunner.getFlowFilesForRelationship(DecryptArchive.REL_SUCCESS);
        for (final MockFlowFile flowFile : unpacked) {
            final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
            final String folder = flowFile.getAttribute(CoreAttributes.PATH.key());
            final Path path = dataPath.resolve(folder).resolve(filename);
            assertTrue(Files.exists(path));

            flowFile.assertContentEquals(path.toFile());
        }
    }

    @Test
    public void testUnpackInvalidZip() throws IOException {
        testRunner.setProperty(DecryptArchive.MODE, DecryptArchive.DECRYPT_UNPACK_MODE);
        testRunner.setProperty(DecryptArchive.PASSWORD, "SOMEPASSWORD");
        testRunner.enqueue(dataPath.resolve("invalid_data.zip"));
        testRunner.enqueue(dataPath.resolve("invalid_data.zip"));
        testRunner.run(2);

        testRunner.assertTransferCount(DecryptArchive.REL_FAILURE, 2);
        testRunner.assertTransferCount(DecryptArchive.REL_ORIGINAL, 0);
        testRunner.assertTransferCount(DecryptArchive.REL_SUCCESS, 0);

        final List<MockFlowFile> unpacked = testRunner.getFlowFilesForRelationship(DecryptArchive.REL_FAILURE);
        for (final MockFlowFile flowFile : unpacked) {
            final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
            // final String folder = flowFile.getAttribute(CoreAttributes.PATH.key());
            final Path path = dataPath.resolve(filename);
            assertTrue(Files.exists(path));

            flowFile.assertContentEquals(path.toFile());
        }
    }

    @Test
    public void testUnpackZipWithFilter() throws IOException {
        testRunner.setProperty(DecryptArchive.MODE, DecryptArchive.DECRYPT_UNPACK_MODE);
        testRunner.setProperty(DecryptArchive.PASSWORD, "SOMEPASSWORD");
        testRunner.setProperty(DecryptArchive.FILE_FILTER, "^folder/date.txt$");
        testRunner.enqueue(dataPath.resolve("protected_data.zip"));
        testRunner.enqueue(dataPath.resolve("protected_data.zip"));
        testRunner.run(2);

        testRunner.assertTransferCount(DecryptArchive.REL_SUCCESS, 2);
        testRunner.assertTransferCount(DecryptArchive.REL_ORIGINAL, 2);
        testRunner.getFlowFilesForRelationship(DecryptArchive.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "1");
        testRunner.getFlowFilesForRelationship(DecryptArchive.REL_ORIGINAL).get(1).assertAttributeEquals(FRAGMENT_COUNT, "1");
        testRunner.assertTransferCount(DecryptArchive.REL_FAILURE, 0);

        List<MockFlowFile> unpacked = testRunner.getFlowFilesForRelationship(DecryptArchive.REL_SUCCESS);
        for (final MockFlowFile flowFile : unpacked) {
            final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
            final String folder = flowFile.getAttribute(CoreAttributes.PATH.key());
            final Path path = dataPath.resolve(folder).resolve(filename);
            assertTrue(Files.exists(path));
            assertEquals("date.txt", filename);
            flowFile.assertContentEquals(path.toFile());
        }
    }

    @Test
    public void testUnpackZipHandlesBadPassword() throws IOException {
        testRunner.setProperty(DecryptArchive.MODE, DecryptArchive.DECRYPT_UNPACK_MODE);
        testRunner.setProperty(DecryptArchive.PASSWORD, "BADPASSWORD");

        testRunner.enqueue(dataPath.resolve("protected_data.zip"));
        testRunner.run();

        testRunner.assertTransferCount(DecryptArchive.REL_SUCCESS, 0);
        testRunner.assertTransferCount(DecryptArchive.REL_ORIGINAL, 0);
        testRunner.assertTransferCount(DecryptArchive.REL_FAILURE, 1);
    }

    @Test
    public void testUnpackZipHandlesBadData() throws IOException {
        testRunner.setProperty(DecryptArchive.MODE, DecryptArchive.DECRYPT_UNPACK_MODE);
        testRunner.setProperty(DecryptArchive.PASSWORD, "SOMEPASSWORD");

        testRunner.enqueue(dataPath.resolve("data.tar"));
        testRunner.run();

        testRunner.assertTransferCount(DecryptArchive.REL_SUCCESS, 0);
        testRunner.assertTransferCount(DecryptArchive.REL_ORIGINAL, 0);
        testRunner.assertTransferCount(DecryptArchive.REL_FAILURE, 1);
    }

    @Test
    public void testDecryptZip() throws IOException {
        testRunner.setProperty(DecryptArchive.MODE, DecryptArchive.DECRYPT_ONLY_MODE);
        testRunner.setProperty(DecryptArchive.PASSWORD, "SOMEPASSWORD");
        testRunner.enqueue(dataPath.resolve("protected_data.zip"));
        testRunner.enqueue(dataPath.resolve("protected_data.zip"));
        testRunner.run(2);

        testRunner.assertTransferCount(DecryptArchive.REL_SUCCESS, 2);
        testRunner.assertTransferCount(DecryptArchive.REL_ORIGINAL, 2);
        testRunner.assertTransferCount(DecryptArchive.REL_FAILURE, 0);

        List<MockFlowFile> decrypted = testRunner.getFlowFilesForRelationship(DecryptArchive.REL_SUCCESS);
        for (final MockFlowFile flowFile : decrypted) {
            final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
            assertEquals("protected_data.zip", filename);
        }

        final List<MockFlowFile> originals = testRunner.getFlowFilesForRelationship(DecryptArchive.REL_ORIGINAL);
        for (final MockFlowFile flowFile : originals) {
            final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
            final Path path = dataPath.resolve(filename);
            assertTrue(Files.exists(path));
            flowFile.assertContentEquals(path.toFile());
        }
    }

    @Test
    public void testDecryptInvalidZip() throws IOException {
        testRunner.setProperty(DecryptArchive.MODE, DecryptArchive.DECRYPT_ONLY_MODE);
        testRunner.setProperty(DecryptArchive.PASSWORD, "SOMEPASSWORD");
        testRunner.enqueue(dataPath.resolve("invalid_data.zip"));
        testRunner.enqueue(dataPath.resolve("invalid_data.zip"));
        testRunner.run(2);

        testRunner.assertTransferCount(DecryptArchive.REL_FAILURE, 2);
        testRunner.assertTransferCount(DecryptArchive.REL_ORIGINAL, 0);
        testRunner.assertTransferCount(DecryptArchive.REL_SUCCESS, 0);

        final List<MockFlowFile> failed = testRunner.getFlowFilesForRelationship(DecryptArchive.REL_FAILURE);
        for (final MockFlowFile flowFile : failed) {
            final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
            final Path path = dataPath.resolve(filename);
            assertTrue(Files.exists(path));

            flowFile.assertContentEquals(path.toFile());
        }
    }

    @Test
    public void testDecryptZipWithFilter() throws IOException {
        testRunner.setProperty(DecryptArchive.MODE, DecryptArchive.DECRYPT_ONLY_MODE);
        testRunner.setProperty(DecryptArchive.PASSWORD, "SOMEPASSWORD");
        testRunner.setProperty(DecryptArchive.FILE_FILTER, "^folder/date.txt$");
        testRunner.enqueue(dataPath.resolve("protected_data.zip"));
        testRunner.enqueue(dataPath.resolve("protected_data.zip"));
        testRunner.run(2);

        testRunner.assertTransferCount(DecryptArchive.REL_SUCCESS, 2);
        testRunner.assertTransferCount(DecryptArchive.REL_ORIGINAL, 2);
        testRunner.assertTransferCount(DecryptArchive.REL_FAILURE, 0);

        List<MockFlowFile> decrypted = testRunner.getFlowFilesForRelationship(DecryptArchive.REL_SUCCESS);
        for (final MockFlowFile flowFile : decrypted) {
            final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
            assertEquals("protected_data.zip", filename);
        }

        final List<MockFlowFile> originals = testRunner.getFlowFilesForRelationship(DecryptArchive.REL_ORIGINAL);
        for (final MockFlowFile flowFile : originals) {
            final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
            final Path path = dataPath.resolve(filename);
            assertTrue(Files.exists(path));
            flowFile.assertContentEquals(path.toFile());
        }
    }

    @Test
    public void testDecryptZipHandlesBadPassword() throws IOException {
        testRunner.setProperty(DecryptArchive.MODE, DecryptArchive.DECRYPT_ONLY_MODE);
        testRunner.setProperty(DecryptArchive.PASSWORD, "BADPASSWORD");

        testRunner.enqueue(dataPath.resolve("protected_data.zip"));
        testRunner.run();

        testRunner.assertTransferCount(DecryptArchive.REL_SUCCESS, 0);
        testRunner.assertTransferCount(DecryptArchive.REL_ORIGINAL, 0);
        testRunner.assertTransferCount(DecryptArchive.REL_FAILURE, 1);
    }

    @Test
    public void testDecryptZipHandlesBadData() throws IOException {
        testRunner.setProperty(DecryptArchive.MODE, DecryptArchive.DECRYPT_ONLY_MODE);
        testRunner.setProperty(DecryptArchive.PASSWORD, "SOMEPASSWORD");

        testRunner.enqueue(dataPath.resolve("data.tar"));
        testRunner.run();

        testRunner.assertTransferCount(DecryptArchive.REL_SUCCESS, 0);
        testRunner.assertTransferCount(DecryptArchive.REL_ORIGINAL, 0);
        testRunner.assertTransferCount(DecryptArchive.REL_FAILURE, 1);
    }
}
