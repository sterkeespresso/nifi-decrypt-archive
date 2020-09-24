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
package sterkeespresso.nifi.processors;

import net.lingala.zip4j.io.inputstream.ZipInputStream;
import net.lingala.zip4j.io.outputstream.ZipOutputStream;
import net.lingala.zip4j.model.LocalFileHeader;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.model.enums.CompressionMethod;
import org.apache.nifi.processor.io.StreamCallback;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

public class Zip4jEncryptor implements DecryptArchive.Encryptor {
    private char[] password;

    public Zip4jEncryptor(final char[] password) {
        this.password = password;
    }

    @Override
    public StreamCallback getEncryptionCallback() throws Exception {
        return null;
    }

    @Override
    public StreamCallback getDecryptionCallback() throws Exception {
        return new Zip4jDecryptCallback(password);
    }

    @Override
    public void updateAttributes(Map<String, String> attributes) {

    }

    private static class Zip4jDecryptCallback implements StreamCallback {
        private char[] password;

        Zip4jDecryptCallback(final char[] password) {
            this.password = password;
        }

        @Override
        public void process(InputStream in, OutputStream out) throws IOException {
            ZipOutputStream zipOut = new ZipOutputStream(out);
            ZipParameters zipParameters = new ZipParameters();
            zipParameters.setCompressionMethod(CompressionMethod.STORE);
            zipParameters.setEncryptFiles(false);

            LocalFileHeader zipEntry;
            byte[] readBuffer = new byte[4096];
            int readLen;

            try (final ZipInputStream zipIn = new ZipInputStream(in, password)) {
                while ((zipEntry = zipIn.getNextEntry()) != null) {
                    zipParameters.setEntrySize(zipEntry.getUncompressedSize());
                    zipParameters.setFileNameInZip(zipEntry.getFileName());
                    zipOut.putNextEntry(zipParameters);

                    while ((readLen = zipIn.read(readBuffer)) != -1) {
                        zipOut.write(readBuffer, 0, readLen);
                    }

                    zipOut.closeEntry();
                }
            }
        }
    }
}
