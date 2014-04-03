package org.apache.gora.cascading.tap.hadoop;

import java.io.IOException;

import org.apache.gora.cascading.tap.AbstractGoraTap;
import org.apache.gora.persistency.Persistent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

@SuppressWarnings({ "rawtypes" })
public class GoraTap extends AbstractGoraTap<JobConf, RecordReader, OutputCollector, GoraScheme> {

    public static final Logger LOG = LoggerFactory.getLogger(GoraTap.class);
    
    public GoraTap(Class<?> keyClass, Class<? extends Persistent> persistentClass, GoraScheme scheme) {
        this(keyClass, persistentClass, scheme, SinkMode.KEEP) ;
    }

    public GoraTap(Class<?> keyClass, Class<? extends Persistent> persistentClass, GoraScheme scheme, SinkMode sinkMode) {
        this(keyClass, persistentClass, scheme, SinkMode.KEEP, new Configuration()) ;
    }
    
    public GoraTap(Class<?> keyClass, Class<? extends Persistent> persistentClass, GoraScheme scheme, Configuration configuration) {
        this(keyClass, persistentClass, scheme, SinkMode.KEEP, configuration) ;
    }
    
    public GoraTap (Class<?> keyClass, Class<? extends Persistent> persistentClass, GoraScheme scheme, SinkMode sinkMode, Configuration configuration) {
        super(keyClass, persistentClass, scheme, sinkMode, configuration) ;
    }

    @Override
    public TupleEntryIterator openForRead(FlowProcess<JobConf> flowProcess, RecordReader input) throws IOException {
        return new HadoopTupleEntrySchemeIterator(flowProcess, this, input) ;
    }

    @Override
    public TupleEntryCollector openForWrite(FlowProcess<JobConf> flowProcess, OutputCollector output) throws IOException {
        GoraTupleEntrySchemeCollector goraTupleCollector = new GoraTupleEntrySchemeCollector(flowProcess, this) ;
        goraTupleCollector.prepare() ;
        return goraTupleCollector ;
    }

}
