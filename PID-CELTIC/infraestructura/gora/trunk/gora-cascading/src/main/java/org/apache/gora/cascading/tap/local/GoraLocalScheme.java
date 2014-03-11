package org.apache.gora.cascading.tap.local;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.gora.mapreduce.GoraInputFormat;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.Query;
import org.apache.gora.query.impl.ResultBase;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Job;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

@SuppressWarnings("rawtypes")
public class GoraLocalScheme extends Scheme<Properties,  // Config
                                       ResultBase,       // Input
                                       OutputCollector,  // Output
                                       Object[],         // SourceContext
                                       Object[]> {       // SinkContext

    Object queryStartKey ;
    Object queryEndKey ;
    Long queryLimit ;
                                           
    private static final long serialVersionUID = 1L;

    public GoraLocalScheme() {
        this(/*source fields*/ Fields.ALL, /*sink fields*/ Fields.ALL, /*numSinkParts*/ 0) ;
    }

    /**
     * Constructor with the fields to load
     * @param sourceSinkFields {@link Fields#ALL} | Strings with fields names from defined in .avsc model (only from 1st level).
     */
    public GoraLocalScheme(Fields sourceSinkFields) {
        this(sourceSinkFields, sourceSinkFields, /*numSinkParts*/ 0) ;
    }

    /**
     * Constructor with the fields to load
     * @param sourceFields {@link Fields#ALL} | Strings with fields names from defined in .avsc model (only from 1st level).
     * @param sumSinkParts {@link Scheme}
     */
    public GoraLocalScheme(Fields sourceFields, int numSinkParts) {
        this(sourceFields, /*sink fields*/ null, numSinkParts) ;
    }

    /**
     * Constructor with the fields to load and fields to save
     * @param sourceFields Strings with fields names from defined in .avsc model (only from 1st level).
     * @param sinkFields Strings with fields names from defined in .avsc model (only from 1st level).
     */
    public GoraLocalScheme(Fields sourceFields, Fields sinkFields) {
        this(sourceFields, sinkFields, /*numSinkParts*/ 0) ;
    }

    /**
     * Constructor with the fields to load, fields to save to and suggestion of number of sink parts
     * @param sourceFields Strings with fields names from defined in .avsc model (only from 1st level).
     * @param sinkFields Strings with fields names from defined in .avsc model (only from 1st level).
     * @param sumSinkParts {@link Scheme}
     */
    public GoraLocalScheme(Fields sourceFields, Fields sinkFields, int numSinkParts) {
        super(sourceFields, sinkFields, numSinkParts);
    }

    public Object getQueryStartKey() {
        return queryStartKey;
    }

    public void setQueryStartKey(Object queryStartKey) {
        this.queryStartKey = queryStartKey;
    }

    public Object getQueryEndKey() {
        return queryEndKey;
    }

    public void setQueryEndKey(Object queryEndKey) {
        this.queryEndKey = queryEndKey;
    }

    public Long getQueryLimit() {
        return queryLimit;
    }

    public void setQueryLimit(Long queryLimit) {
        this.queryLimit = queryLimit;
    }

    @SuppressWarnings("unchecked")
    public Query<?,? extends Persistent> createQuery(GoraLocalTap tap) {
        
        Query query = tap.getDataStore(null).newQuery() ;
        
        if (this.queryStartKey != null)
            query.setStartKey(this.queryStartKey) ;
        if (this.queryEndKey != null)
            query.setEndKey(this.queryEndKey) ;
        if (this.queryLimit != null)
            query.setLimit(this.queryLimit) ;
        
        if (this.getSourceFields().isDefined()) {
            String[] fieldNames = (String []) this.getSourceFields().
            query.setFields(null)
        }
        
        query.setFields(fieldNames) ;
    }
    
    @Override
    public void sourceConfInit(FlowProcess<Properties> flowProcess, Tap<Properties, ResultBase, OutputCollector> tap, Properties propsConf) {
    }

    @Override
    public void sinkConfInit(FlowProcess<Properties> flowProcess, Tap<Properties, ResultBase, OutputCollector> tap, Properties propsConf) {
    }
    
    @Override
    public boolean source(FlowProcess<Properties> flowProcess, SourceCall<Object[], ResultBase> sourceCall) throws IOException {
        String key = null ;
        PersistentBase value = null ;
        
        try {
            if (!sourceCall.getInput().next()) {
                return false ;
            }
        } catch (Exception e) {
            // TODO: spit to log
            throw new IOException(e) ;
        }
        
        Tuple tuple = sourceCall.getIncomingEntry().getTuple();
        tuple.clear();
        tuple.addString(key) ;
        tuple.add(value) ;
        
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void sink(FlowProcess<Properties> flowProcess, SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
        Fields fields = sinkCall.getOutgoingEntry().getFields() ;

        String key = (String)fields.get(0) ;
        PersistentBase persistent = (PersistentBase) fields.get(1) ;

        sinkCall.getOutput().collect(key, persistent) ;
    }
    
}
