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

package org.apache.nifi.processors.record;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.*;
import org.apache.nifi.serialization.record.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@EventDriven
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@SideEffectFree
@Tags({"remap", "rename", "fields", "columns", "schema", "json", "csv", "avro", "log", "logs", "freeform", "text"})
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer for the FlowFiles routed to the 'splits' Relationship."),
        @WritesAttribute(attribute = "record.count", description = "The number of records in the FlowFile. This is added to FlowFiles that are routed to the 'splits' Relationship."),
        @WritesAttribute(attribute = "segment.original.filename ", description = "The filename of the parent FlowFile")
})
@CapabilityDescription("Remaps fields of a cording to lookup service")
public class RemapRecord extends AbstractProcessor {

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .description("Specifies the Controller Service to use for reading incoming data")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();
    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .description("Specifies the Controller Service to use for writing out the records")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor LOOKUP_SERVICE = new PropertyDescriptor.Builder()
            .name("lookup-service")
            .displayName("Lookup Service")
            .description("The Lookup Service to use in order to lookup the new field name base on the old field name for the Record")
            .identifiesControllerService(LookupService.class)
            .required(true)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The Flowfile successfully remapped.")
            .build();
    static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("Upon successfully splitting an input FlowFile, the original FlowFile will be sent to this relationship.")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile cannot be transformed from the configured input format to the configured output format, "
                    + "the unchanged FlowFile will be routed to this relationship.")
            .build();
    private volatile LookupService<?> lookupService;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        this.lookupService = context.getProperty(LOOKUP_SERVICE).asControllerService(LookupService.class);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(RECORD_READER);
        properties.add(RECORD_WRITER);
        properties.add(LOOKUP_SERVICE);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        final AtomicReference<FlowFile> success = new AtomicReference<>(session.create(original));
        final Map<String, String> originalAttributes = original.getAttributes();

        try {
            session.read(original, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    try (final RecordReader reader = readerFactory.createRecordReader(originalAttributes, in, original.getSize(), getLogger())) {

                        final RecordSchema schema = writerFactory.getSchema(originalAttributes, reader.getSchema());

                        final RecordSet recordSet = new RemapRecordSet(reader.createRecordSet(),lookupService);

                        final Map<String, String> attributes = new HashMap<>();
                        final WriteResult writeResult;
                        try (final OutputStream out = session.write(success.get());
                             final RecordSetWriter writer = writerFactory.createWriter(getLogger(), schema, out, success.get())) {
                            writeResult = writer.write(recordSet);
                            attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                            attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                            attributes.putAll(writeResult.getAttributes());
                            success.set(session.putAllAttributes(success.get(), attributes));
                        }
                        session.getProvenanceReporter().route(success.get(), REL_SUCCESS);
                        session.transfer(success.get(), REL_SUCCESS);
                    } catch (final SchemaNotFoundException | MalformedRecordException e) {
                        throw new ProcessException("Failed to parse incoming data", e);
                    }
                }
            });
        } catch (final ProcessException pe) {
            getLogger().error("Failed to remap {}", new Object[] {original, pe});
            session.remove(success.get());
            session.transfer(original, REL_FAILURE);
            return;
        }
        session.transfer(original, REL_ORIGINAL);
        session.transfer(success.get(), REL_SUCCESS);
        getLogger().info("Successfully remapped {}", new Object[] {original});
    }

    class RemapRecordSet implements RecordSet{
        private final RecordSet recordSet;
        private final SimpleRecordSchema schema;
        private final LookupService<?> lookupService;
        final Map<String, Object> lookupCoordinates = new HashMap<>(1);
        final String coordinateKey;

        HashMap<String, String> cache = new HashMap<>();


        public RemapRecordSet(RecordSet recordSet, LookupService<?> lookupService) throws IOException {
            this.recordSet = recordSet;
            this.lookupService = lookupService;
            this.coordinateKey = lookupService.getRequiredKeys().iterator().next();


            List<RecordField> fields = new ArrayList<RecordField>();
            for (RecordField field : recordSet.getSchema().getFields()) {
                fields.add(new RecordField(lookup(field.getFieldName()), field.getDataType()));
            }
            this.schema = new SimpleRecordSchema(fields);
        }

        public String lookup(String key) {
            String o = cache.get(key);
            if (o != null) {
                return o;
            }
            lookupCoordinates.clear();
            lookupCoordinates.put(coordinateKey, key);
            Optional<?> b = Optional.empty();
            try {
                b = lookupService.lookup(lookupCoordinates);
            } catch (LookupFailureException e) {
                getLogger().info("Lookup failure for {} (key:{}) (id:{}) (err:{})", lookupCoordinates, key, getIdentifier(), e);
            }

            if (b.isPresent()){
                o = (String)b.get();
                cache.put(key, o);
            }
            return o;
        }

        @Override
        public RecordSchema getSchema() {
            return schema;
        }

        @Override
        public Record next() throws IOException {
            Map<String, Object> m1 = recordSet.next().toMap();
            Map<String, Object> m2 = new HashMap<>();
            m1.forEach((k, v) -> m2.put(lookup(k),v));
            return new MapRecord(schema, m2);

        }
    }
}
