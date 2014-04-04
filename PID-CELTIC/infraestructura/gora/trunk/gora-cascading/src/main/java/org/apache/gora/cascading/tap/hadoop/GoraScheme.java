package org.apache.gora.cascading.tap.hadoop;

import java.io.IOException;

import org.apache.avro.util.Utf8;
import org.apache.gora.cascading.tap.AbstractGoraScheme;
import org.apache.gora.cascading.util.ConfigurationUtil;
import org.apache.gora.mapreduce.GoraInputFormat;
import org.apache.gora.mapreduce.GoraOutputFormat;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.Query;
import org.apache.gora.store.DataStore;
import org.apache.gora.util.GoraException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

import com.twitter.elephantbird.mapred.input.DeprecatedInputFormatWrapper;
import com.twitter.elephantbird.mapred.output.DeprecatedOutputFormatWrapper;

@SuppressWarnings("rawtypes")
public class GoraScheme extends AbstractGoraScheme<JobConf, // Config
                                       RecordReader,     // Input
                                       OutputCollector,  // Output
                                       Object[],         // SourceContext
                                       Object[],         // SinkContext
                                       GoraTap>          // Tap class
{ 

    public static final Logger LOG = LoggerFactory.getLogger(GoraScheme.class);
    
    public GoraScheme() {
        this(/*source fields*/ Fields.ALL, /*sink fields*/ Fields.ALL, /*numSinkParts*/ 0) ;
    }

    /**
     * Constructor with the fields to load
     * @param sourceSinkFields {@link Fields#ALL} | Strings with fields names from defined in .avsc model (only from 1st level).
     */
    public GoraScheme(Fields sourceSinkFields) {
        this(sourceSinkFields, sourceSinkFields, /*numSinkParts*/ 0) ;
    }

    /**
     * Constructor with the fields to load
     * @param sourceFields {@link Fields#ALL} | Strings with fields names from defined in .avsc model (only from 1st level).
     * @param sumSinkParts {@link Scheme}
     */
    public GoraScheme(Fields sourceFields, int numSinkParts) {
        this(sourceFields, /*sink fields*/ Fields.ALL, numSinkParts) ;
    }
    
    /**
     * Constructor with the fields to load and fields to save
     * @param sourceFields Strings with fields names from defined in .avsc model (only from 1st level).
     * @param sinkFields Strings with fields names from defined in .avsc model (only from 1st level).
     */
    public GoraScheme(Fields sourceFields, Fields sinkFields) {
        this(sourceFields, sinkFields, /*numSinkParts*/ 0) ;
    }

    /**
     * Constructor with the fields to load, fields to save to and suggestion of number of sink parts
     * @param sourceFields Strings with fields names from defined in .avsc model (only from 1st level).
     * @param sinkFields Strings with fields names from defined in .avsc model (only from 1st level).
     * @param sumSinkParts {@link Scheme}
     */
    public GoraScheme(Fields sourceFields, Fields sinkFields, int numSinkParts) {
        this(sourceFields, false, sinkFields, false, 0) ;
    }
    
    /**
     * Constructor with source and sink fields that enables sourcing/sinking the Persistent instance
     * as tuples with format ("key","persistent").
     * 
     * @see GoraScheme#setTupleKeyName(String)
     * @see GoraScheme#setTuplePersistentFieldName(String)
     * 
     * @param sourceFields
     * @param sourceAsPersistent if true, with source ("key","persistent")
     * @param sinkFields
     * @param sinkAsPersistent if true, will sink ("key","persistent")
     */
    public GoraScheme(Fields sourceFields, boolean sourceAsPersistent, Fields sinkFields, boolean sinkAsPersistent) {
        this(sourceFields, sourceAsPersistent, sinkFields, sinkAsPersistent, 0) ;
    }

    /**
     * Constructor with source and sink fields that enables sourcing/sinking the Persistent instance
     * as tuples with format ("key","persistent").
     * 
     * @see GoraScheme#setTupleKeyName(String)
     * @see GoraScheme#setTuplePersistentFieldName(String)
     * 
     * @param sourceFields
     * @param sourceAsPersistent if true, with source ("key","persistent")
     * @param sinkFields
     * @param sinkAsPersistent if true, will sink ("key","persistent")
     */
    public GoraScheme(Fields sourceFields, boolean sourceAsPersistent, Fields sinkFields, boolean sinkAsPersistent, int numSinkParts) {
        super(sourceFields, sourceAsPersistent, sinkFields, sinkAsPersistent, numSinkParts) ;
    }

    @Override
    protected String[] getPersistentFields(FlowProcess<JobConf> flowProcess, Tap tap) throws GoraException {
        return ((PersistentBase) ((GoraTap)tap).getDataStore(flowProcess.getConfigCopy()).newPersistent()).getFields() ;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void sourceConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf jobConf) {
        try {
            // Workaround to load Job configuration into JobConf.
            // Configure and ad-hoc Job and merge configuration into jobConf (bellow)
            Job tmpGoraJob = new Job(jobConf);

            DataStore dataStore = ((GoraTap)tap).getDataStore(jobConf) ;
            Query query = dataStore.newQuery() ;
            GoraInputFormat.setInput(tmpGoraJob, query, dataStore, true) ;

            // Copies the configuration set by "setInput()" into jobConf.
            ConfigurationUtil.mergeConfigurationFromTo(tmpGoraJob.getConfiguration(), jobConf) ; 
        } catch (Exception e) {
            throw new RuntimeException("Failed then configuring GoraInputFormat in the job",e) ;
        }
        DeprecatedInputFormatWrapper.setInputFormat(GoraInputFormat.class, jobConf) ;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void sinkConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf jobConf) {
        try {
            // Workaround to load Job configuration into JobConf.
            Job tmpGoraJob = new Job(jobConf);
            
            DataStore dataStore = ((GoraTap)tap).getDataStore(jobConf) ;
            GoraOutputFormat.setOutput(tmpGoraJob, dataStore, true) ;

            // Copies the configuration set by "setOutput()" into jobConf.
            ConfigurationUtil.mergeConfigurationFromTo(tmpGoraJob.getConfiguration(), jobConf) ; 
        } catch (Exception e) {
            throw new RuntimeException("Failed then configuring GoraInputFormat in the job",e) ;
        }
        DeprecatedOutputFormatWrapper.setOutputFormat(GoraOutputFormat.class, jobConf) ;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean source(FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall) throws IOException {
        String key = (String) sourceCall.getInput().createKey() ;
        PersistentBase persistent = (PersistentBase) sourceCall.getInput().createValue() ;
        try {
            if (!sourceCall.getInput().next((Object)key, (Object) persistent)) {
                return false ;
            }
        } catch (Exception e) {
            LOG.error("Error reading next input tuple", e) ;
            throw new IOException(e) ;
        }
        
        TupleEntry tupleEntry = sourceCall.getIncomingEntry() ;

        // Set key field
        tupleEntry.setString(this.getTupleKeyName(), key) ;
        
        if (this.isSourceAsPersistent()) {
            tupleEntry.setObject(this.getTuplePersistentFieldName(), persistent) ;
        } else {
            // scheme has to be ("key", "field", "field", ...)
            for (String fieldName: this.getSourceGoraFields()) {
                setTupleFieldFromPersistent(persistent, tupleEntry, fieldName) ;
            }
        }

        LOG.debug("(Tuple sourced) {}", tupleEntry.toString()) ;
        
        return true;
    }

    /**
     * Saves the tuple in sinbkCall.getOutgoingEntry()
     * 
     * The fields to save are indicated in the constructor as sinkFields, and can be Fields.ALL or
     * specific fields of the Persistent object. 
     * 
     * If the tuple received has fields: ("key", "persistent"), the sencond field must be of type 
     * PersistentBase. Will only save the fields set in sinkFields.
     * 
     * If the tuple has not exactly ("key","persistent") fields, a PersistentBase object will
     * be created, populated with sinkFields values present in the TupleEntry and persisted.
     * 
     */
    @SuppressWarnings("unchecked")
    @Override
    public void sink(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
        OutputCollector collector = sinkCall.getOutput();
        TupleEntry tupleEntry = sinkCall.getOutgoingEntry() ;

        LOG.debug("(Tuple to sink) ", tupleEntry.toString()) ;
        
        String key = tupleEntry.getString(this.getTupleKeyName()) ;

        if (key == null) return ;

        if (this.isSinkAsPersistent()) {
            // tuple ("key","persistent")
            PersistentBase persistent = (PersistentBase) tupleEntry.getObject(this.getTuplePersistentFieldName()) ;

            persistent.clearDirty() ;
            // Set dirty the sink fields of the Persistent instance
            for (String fieldName : this.getSinkGoraFields()) {
                // TODO: Maybe log warn when a field does not exist? (but slower)
                persistent.setDirty(fieldName) ;
            }
            collector.collect(key, persistent) ;
        } else {
            JobConf conf = flowProcess.getConfigCopy() ;
            // tuple (key, field, field,...)
            PersistentBase newOutputPersistent =  (PersistentBase) (((GoraTupleEntrySchemeCollector) sinkCall.getOutput()).getTap().getDataStore(conf).newPersistent()) ;
            
            // Copy each field of the tuple to the Persistent instance
            for (String fieldName : this.getSinkGoraFields()) {
                try {
                    int indexInPersistent = newOutputPersistent.getFieldIndex(fieldName) ;
                    Object value = tupleEntry.getObject(fieldName) ;
                    
                    // convert String -> Utf8
                    if (value instanceof String){
                        value = new Utf8((String)value) ;
                    }
                    if (value instanceof Integer) {
                        value = ((Integer) value).longValue() ;
                    }
                    newOutputPersistent.put(indexInPersistent, value) ;
                } catch(NullPointerException e) {
                    LOG.warn("Field <{}> not found in sink class {}", fieldName, newOutputPersistent.getClass().getName()) ;
                }
                
            }
            collector.collect(key, newOutputPersistent) ;
        }

    }

}
