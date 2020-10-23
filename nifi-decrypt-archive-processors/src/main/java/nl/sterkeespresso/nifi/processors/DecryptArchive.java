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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import net.lingala.zip4j.io.inputstream.ZipInputStream;
import net.lingala.zip4j.model.LocalFileHeader;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;

@Tags({"decryption", "password", "unpack", "un-merge", "zip", "archive"})
@CapabilityDescription("Decrypts and optionally unpacks the content of FlowFiles that have been packaged and encrypted using the zip format.")
@ReadsAttributes({})
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "If the FlowFile is successfully unpacked, its MIME Type is no longer known, so the mime.type "
                + "attribute is set to application/octet-stream."),
        @WritesAttribute(attribute = "fragment.identifier", description = "All unpacked FlowFiles produced from the same parent FlowFile will have the same randomly generated "
                + "UUID added for this attribute"),
        @WritesAttribute(attribute = "fragment.index", description = "A one-up number that indicates the ordering of the unpacked FlowFiles that were created from a single "
                + "parent FlowFile"),
        @WritesAttribute(attribute = "fragment.count", description = "The number of unpacked FlowFiles generated from the parent FlowFile"),
        @WritesAttribute(attribute = "segment.original.filename ", description = "The filename of the parent FlowFile. Extension .zip is removed because "
                + "the MergeContent processor automatically adds those extensions if it is used to rebuild the original FlowFile")})
@SeeAlso({})
public class DecryptArchive extends AbstractProcessor {

    public static final String DECRYPT_ONLY_MODE = "Decrypt only";
    public static final String DECRYPT_UNPACK_MODE = "Decrypt and unpack";

    // attribute keys
    public static final String FRAGMENT_ID = FragmentAttributes.FRAGMENT_ID.key();
    public static final String FRAGMENT_INDEX = FragmentAttributes.FRAGMENT_INDEX.key();
    public static final String FRAGMENT_COUNT = FragmentAttributes.FRAGMENT_COUNT.key();
    public static final String SEGMENT_ORIGINAL_FILENAME = FragmentAttributes.SEGMENT_ORIGINAL_FILENAME.key();

    public static final String OCTET_STREAM = "application/octet-stream";

    public static final PropertyDescriptor MODE = new PropertyDescriptor.Builder()
            .name("Mode")
            .displayName("Mode")
            .description("Specifies whether the archive should only be decrypted or also be unpacked")
            .required(true)
            .allowableValues(DECRYPT_ONLY_MODE, DECRYPT_UNPACK_MODE)
            .defaultValue(DECRYPT_ONLY_MODE)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .displayName("Password")
            .description("The Password to use for decrypting the archive")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor FILE_FILTER = new PropertyDescriptor.Builder()
            .name("File Filter")
            .description("Only files contained in the archive whose names match the given regular expression will be processed (unpack only)")
            .required(true)
            .defaultValue(".*")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Decrypted FlowFiles are sent to this relationship")
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original FlowFile is sent to this relationship after it has been successfully decrypted")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("The original FlowFile is sent to this relationship when it cannot be decrypted for some reason")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(MODE);
        descriptors.add(PASSWORD);
        descriptors.add(FILE_FILTER);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final String mode = context.getProperty(MODE).getValue();
        final Pattern filter = Pattern.compile(context.getProperty(FILE_FILTER).getValue());
        final String password = context.getProperty(PASSWORD).getValue();

        switch (mode) {
            case DECRYPT_ONLY_MODE:
                Encryptor encryptor;
                StreamCallback callback;
                FlowFile decrypted = session.clone(flowFile);

                try {
                    encryptor = new Zip4jEncryptor(password.toCharArray());
                    callback = encryptor.getDecryptionCallback();
                } catch (final Exception e) {
                    logger.error("Failed to initialize decryption algorithm because - ", new Object[]{e});
                    session.rollback();
                    context.yield();
                    return;
                }

                try {
                    final StopWatch stopWatch = new StopWatch(true);
                    decrypted = session.write(decrypted, callback);

                    if (decrypted.getSize() == 0) {
                        logger.error("Unable to decrypt {} because it does not appear to have any entries; routing to failure", new Object[]{flowFile});
                        session.transfer(flowFile, REL_FAILURE);
                        session.remove(decrypted);
                        return;
                    }

                    // Update the flowfile attributes
                    Map<String, String> clonedAttributes = new HashMap<>(flowFile.getAttributes());
                    encryptor.updateAttributes(clonedAttributes);
                    decrypted = session.putAllAttributes(decrypted, clonedAttributes);

                    session.getProvenanceReporter().modifyContent(decrypted, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                    session.transfer(decrypted, REL_SUCCESS);
                    session.transfer(flowFile, REL_ORIGINAL);
                    logger.info("Successfully decrypted {} to {}", new Object[]{flowFile, decrypted});
                } catch (final ProcessException e) {
                    logger.error("Cannot decrypt {} - ", new Object[]{flowFile, e});
                    session.transfer(flowFile, REL_FAILURE);
                    session.remove(decrypted);
                }
                break;

            case DECRYPT_UNPACK_MODE:
                final Unpacker unpacker = new Zip4jUnpacker(filter, password.toCharArray());
                final List<FlowFile> unpacked = new ArrayList<>();
                try {
                    unpacker.unpack(session, flowFile, unpacked);
                    if (unpacked.isEmpty()) {
                        logger.error("Unable to unpack {} because it does not appear to have any entries; routing to failure", new Object[]{flowFile});
                        session.transfer(flowFile, REL_FAILURE);
                        return;
                    }

                    finishFragmentAttributes(session, flowFile, unpacked);
                    session.transfer(unpacked, REL_SUCCESS);
                    final String fragmentId = unpacked.size() > 0 ? unpacked.get(0).getAttribute(FRAGMENT_ID) : null;
                    flowFile = FragmentAttributes.copyAttributesToOriginal(session, flowFile, fragmentId, unpacked.size());
                    session.transfer(flowFile, REL_ORIGINAL);
                    session.getProvenanceReporter().fork(flowFile, unpacked);
                    logger.info("Unpacked {} into {} and transferred to success", new Object[]{flowFile, unpacked});
                } catch (final Exception e) {
                    logger.error("Unable to unpack {} due to {}; routing to failure", new Object[]{flowFile, e});
                    session.transfer(flowFile, REL_FAILURE);
                    session.remove(unpacked);
                }
                break;
        }
    }

    private void finishFragmentAttributes(final ProcessSession session, final FlowFile source, final List<FlowFile> unpacked) {
        // first pass verifies all FlowFiles have the FRAGMENT_INDEX attribute and gets the total number of fragments
        int fragmentCount = 0;
        for (FlowFile ff : unpacked) {
            String fragmentIndex = ff.getAttribute(FRAGMENT_INDEX);
            if (fragmentIndex != null) {
                fragmentCount++;
            } else {
                return;
            }
        }

        String originalFilename = source.getAttribute(CoreAttributes.FILENAME.key());
        if (originalFilename.endsWith(".zip")) {
            originalFilename = originalFilename.substring(0, originalFilename.length() - 4);
        }

        // second pass adds fragment attributes
        ArrayList<FlowFile> newList = new ArrayList<>(unpacked);
        unpacked.clear();
        for (FlowFile ff : newList) {
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(FRAGMENT_COUNT, String.valueOf(fragmentCount));
            attributes.put(SEGMENT_ORIGINAL_FILENAME, originalFilename);
            FlowFile newFF = session.putAllAttributes(ff, attributes);
            unpacked.add(newFF);
        }
    }

    public interface Encryptor {
        StreamCallback getEncryptionCallback() throws Exception;

        StreamCallback getDecryptionCallback() throws Exception;

        void updateAttributes(Map<String, String> attributes);
    }

    private static abstract class Unpacker {
        private Pattern fileFilter = null;

        public Unpacker() {
        }

        public Unpacker(Pattern fileFilter) {
            this.fileFilter = fileFilter;
        }

        abstract void unpack(ProcessSession session, FlowFile source, List<FlowFile> unpacked);

        protected boolean fileMatches(ArchiveEntry entry) {
            return fileFilter == null || fileFilter.matcher(entry.getName()).find();
        }
    }

    private static class Zip4jUnpacker extends Unpacker {
        private Pattern fileFilter = null;
        private char[] password;

        public Zip4jUnpacker(Pattern fileFilter, final char[] password) {
            super(fileFilter);

            this.fileFilter = fileFilter;
            this.password = password;
        }

        @Override
        public void unpack(final ProcessSession session, final FlowFile source, final List<FlowFile> unpacked) {
            final String fragmentId = UUID.randomUUID().toString();
            session.read(source, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    int fragmentCount = 0;
                    LocalFileHeader zipEntry;

                    try (final ZipInputStream zipIn = new ZipInputStream(in, password)) {
                        while ((zipEntry = zipIn.getNextEntry()) != null) {
                            if (zipEntry.isDirectory() || !fileMatches(zipEntry)) {
                                continue;
                            }
                            final File file = new File(zipEntry.getFileName());
                            final String parentDirectory = (file.getParent() == null) ? "/" : file.getParent();
                            final Path absPath = file.toPath().toAbsolutePath();
                            final String absPathString = absPath.getParent().toString() + "/";

                            FlowFile unpackedFile = session.create(source);
                            try {
                                final Map<String, String> attributes = new HashMap<>();
                                attributes.put(CoreAttributes.FILENAME.key(), file.getName());
                                attributes.put(CoreAttributes.PATH.key(), parentDirectory);
                                attributes.put(CoreAttributes.ABSOLUTE_PATH.key(), absPathString);
                                attributes.put(CoreAttributes.MIME_TYPE.key(), OCTET_STREAM);

                                attributes.put(FRAGMENT_ID, fragmentId);
                                attributes.put(FRAGMENT_INDEX, String.valueOf(++fragmentCount));

                                unpackedFile = session.putAllAttributes(unpackedFile, attributes);
                                unpackedFile = session.write(unpackedFile, new OutputStreamCallback() {
                                    @Override
                                    public void process(final OutputStream out) throws IOException {
                                        StreamUtils.copy(zipIn, out);
                                    }
                                });
                            } finally {
                                unpacked.add(unpackedFile);
                            }
                        }
                    }
                }
            });
        }

        protected boolean fileMatches(LocalFileHeader entry) {
            return fileFilter == null || fileFilter.matcher(entry.getFileName()).find();
        }
    }
}