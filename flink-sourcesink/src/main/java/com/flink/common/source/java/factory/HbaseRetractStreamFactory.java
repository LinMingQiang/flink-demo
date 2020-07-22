package com.flink.common.source.java.factory;

import com.flink.common.java.tablesink.HbaseRetractStreamTableSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.Schema.*;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;

public class HbaseRetractStreamFactory implements StreamTableSinkFactory<Tuple2<Boolean, Row>> {
    @Override
    public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(Map<String, String> properties) {
        DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);
        TableSchema tableSchema = descriptorProperties.getTableSchema(SCHEMA);
// bridge to java.sql.Timestamp/Time/Date
        DataType[] fieldTypes = Arrays.stream(tableSchema.getFieldDataTypes())
                .map(dt -> {
                    switch (dt.getLogicalType().getTypeRoot()) {
                        case TIMESTAMP_WITHOUT_TIME_ZONE:
                            return dt.bridgedTo(Timestamp.class);
                        case TIME_WITHOUT_TIME_ZONE:
                            return dt.bridgedTo(Time.class);
                        case DATE:
                            return dt.bridgedTo(Date.class);
                        default:
                            return dt;
                    }
                })
                .toArray(DataType[]::new);
        return new HbaseRetractStreamTableSink(tableSchema.getFieldNames(), fieldTypes);
    }

    @Override
    public Map<String, String> requiredContext() {
        HashMap<String, String> context = new HashMap<String, String>();
        context.put(CONNECTOR_TYPE, "hbasesink");
        return context;    }

    @Override
    public List<String> supportedProperties() {
        ArrayList properties = new ArrayList<>();
        // schema
        properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_NAME);
        return properties;    }
}
