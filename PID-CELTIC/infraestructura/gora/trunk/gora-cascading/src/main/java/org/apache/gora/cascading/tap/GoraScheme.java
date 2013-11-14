package org.apache.gora.cascading.tap;

import java.io.IOException;

import org.apache.gora.mapreduce.GoraInputFormat;
import org.apache.gora.mapreduce.GoraInputFormatFactory;
import org.apache.gora.mapreduce.GoraOutputFormatFactory;
import org.apache.gora.mapreduce.GoraRecordReader;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class GoraScheme extends Scheme<JobConf, GoraRecordReader<String, ? extends PersistentBase>, OutputCollector<String,Object>, Object[], Object[]> {

    public GoraScheme() {
        // TODO Auto-generated constructor stub
    }

    /**
     * Constructor with the fields to load
     * @param sourceFields {@link Fields#ALL} | Strings with fields names from defined in .avsc model (only from 1st level).
     */
    public GoraScheme(Fields sourceFields) {
        super(sourceFields);
    }

    /**
     * Constructor with the fields to load
     * @param sourceFields {@link Fields#ALL} | Strings with fields names from defined in .avsc model (only from 1st level).
     * @param sumSinkParts {@link Scheme}
     */
    public GoraScheme(Fields sourceFields, int numSinkParts) {
        super(sourceFields, numSinkParts);
    }

    /**
     * Constructor with the fields to load, fields to save to and suggestion of number of sink parts
     * @param sourceFields Strings with fields names from defined in .avsc model (only from 1st level).
     * @param sinkFields Strings with fields names from defined in .avsc model (only from 1st level).
     */
    public GoraScheme(Fields sourceFields, Fields sinkFields) {
        super(sourceFields, sinkFields);
    }

    /**
     * Constructor with the fields to load, fields to save to and suggestion of number of sink parts
     * @param sourceFields Strings with fields names from defined in .avsc model (only from 1st level).
     * @param sinkFields Strings with fields names from defined in .avsc model (only from 1st level).
     * @param sumSinkParts {@link Scheme}
     */
    public GoraScheme(Fields sourceFields, Fields sinkFields, int numSinkParts) {
        super(sourceFields, sinkFields, numSinkParts);
    }


    @Override
    public void sourceConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, GoraRecordReader<String, ? extends PersistentBase>, OutputCollector<String, Object>> tap, JobConf conf) {
        // TODO Auto-generated method stub
        GoraInputFormatFactory.createInstance(String.class, PersistentBase.class).
        // AAAAAAAAAAAAAAAAAGH!!! CASCADING USES OLD MAPRED API!!!!!
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, GoraRecordReader<String, ? extends PersistentBase>, OutputCollector<String, Object>> tap, JobConf conf) {
        // TODO Auto-generated method stub
        GoraOutputFormat.
    }

    @Override
    public boolean source(FlowProcess<JobConf> flowProcess, SourceCall<Object[], GoraRecordReader<String, ? extends PersistentBase>> sourceCall) throws IOException {
        try {

            if (!sourceCall.getInput().nextKeyValue()) {
                return false ;
            }
        
            Tuple tuple = sourceCall.getIncomingEntry().getTuple();
            tuple.clear();
            tuple.addString(sourceCall.getInput().getCurrentKey()) ;
            tuple.add(sourceCall.getInput().getCurrentValue()) ;

        }catch (InterruptedException e) {
            throw new IOException(e) ;
        }
        
        return true;
    }

    @Override
    public void sink(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector<String, Object>> sinkCall) throws IOException {
        Fields fields = sinkCall.getOutgoingEntry().getFields() ;

        String key = (String)fields.get(0) ;
        PersistentBase persistent = (PersistentBase) fields.get(1) ;

        sinkCall.getOutput().collect(key, persistent) ;
    }

}
