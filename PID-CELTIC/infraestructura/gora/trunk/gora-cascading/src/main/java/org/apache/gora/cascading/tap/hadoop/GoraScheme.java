package org.apache.gora.cascading.tap.hadoop;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.gora.cascading.tap.GoraDeprecatedInputFormatValueCopier;
import org.apache.gora.mapreduce.GoraInputFormat;
import org.apache.gora.mapreduce.GoraOutputFormat;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.Query;
import org.apache.gora.store.DataStore;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.util.GoraException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.Job;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.twitter.elephantbird.mapred.input.DeprecatedInputFormatWrapper;
import com.twitter.elephantbird.mapred.output.DeprecatedOutputFormatWrapper;

@SuppressWarnings("rawtypes")
public class GoraScheme extends Scheme<JobConf,          // Config
                                       RecordReader,     // Input
                                       OutputCollector,  // Output
                                       Object[],         // SourceContext
                                       Object[]> {       // SinkContext

    private static final long serialVersionUID = 1L;

    private Class<? extends Persistent> persistentClass;
    private Class<?> keyClass;
    private DataStore<?, ? extends Persistent> dataStore ;
    private Query<?, ? extends Persistent> query ;
    
// TODO: Cargar filtros, rangos de clave, etc a cargar para sourceConfInit
    
    public GoraScheme(Class<?> keyClass, Class<? extends Persistent> persistentClass) {
        this(keyClass, persistentClass, /*source fields*/ Fields.ALL, /*sink fields*/ Fields.ALL, /*numSinkParts*/ 0) ;
    }

    /**
     * Constructor with the fields to load
     * @param sourceFields {@link Fields#ALL} | Strings with fields names from defined in .avsc model (only from 1st level).
     */
    public GoraScheme(Class<?> keyClass, Class<? extends Persistent> persistentClass, Fields sourceFields) {
        this(keyClass, persistentClass, sourceFields, /*sink fields*/ null, /*numSinkParts*/ 0) ;
    }

    /**
     * Constructor with the fields to load
     * @param sourceFields {@link Fields#ALL} | Strings with fields names from defined in .avsc model (only from 1st level).
     * @param sumSinkParts {@link Scheme}
     */
    public GoraScheme(Class<?> keyClass, Class<? extends Persistent> persistentClass, Fields sourceFields, int numSinkParts) {
        this(keyClass, persistentClass, sourceFields, /*sink fields*/ null, numSinkParts) ;
    }

    /**
     * Constructor with the fields to load and fields to save
     * @param sourceFields Strings with fields names from defined in .avsc model (only from 1st level).
     * @param sinkFields Strings with fields names from defined in .avsc model (only from 1st level).
     */
    public GoraScheme(Class<?> keyClass, Class<? extends Persistent> persistentClass, Fields sourceFields, Fields sinkFields) {
        this(keyClass, persistentClass, sourceFields, sinkFields, /*numSinkParts*/ 0) ;
    }

    /**
     * Constructor with the fields to load, fields to save to and suggestion of number of sink parts
     * @param sourceFields Strings with fields names from defined in .avsc model (only from 1st level).
     * @param sinkFields Strings with fields names from defined in .avsc model (only from 1st level).
     * @param sumSinkParts {@link Scheme}
     */
    public GoraScheme(Class<?> keyClass, Class<? extends Persistent> persistentClass, Fields sourceFields, Fields sinkFields, int numSinkParts) {
        super(sourceFields, sinkFields, numSinkParts);
        this.keyClass = keyClass ;
        this.persistentClass = persistentClass ;
    }

    public Class<?> getKeyClass() {
        return this.keyClass ;
    }
    
    public Class<? extends Persistent> getPersistentClass() {
        return this.persistentClass ;
    }

    /**
     * Retrieves the datastore
     * @param conf (Optional) Needed the first time the datastore is retrieves for this scheme.
     *             In subsequent calls is ignored since the datastore is taken from cache.
     * @return
     * @throws GoraException
     */
    public DataStore<?, ? extends Persistent> getDataStore(JobConf conf) throws GoraException {
        if (this.dataStore == null) {
            this.dataStore = DataStoreFactory.getDataStore(this.keyClass, this.persistentClass, conf) ;
        }
        return this.dataStore ;
    }
    
    @SuppressWarnings("unchecked")
    public void setQuery(Query query) {
        this.query = query ;
    }
    
    @Override
    public void sinkConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf jobConf) {
        // XXX Ugly, but temporary to know why is all this here...
        try {
            // Workaround to load Job configuration into JobConf.
            Job tmpGoraJob = new Job(jobConf);
            GoraOutputFormat.setOutput(tmpGoraJob, this.getDataStore(jobConf), true) ;
            // Copies the configuration set by "setOutput()" into jobConf.
            this.mergeConfigurationFromTo(tmpGoraJob.getConfiguration(), jobConf) ; 
        } catch (Exception e) {
            throw new RuntimeException("Failed then configuring GoraInputFormat in the job",e) ;
        }
        DeprecatedOutputFormatWrapper.setOutputFormat(GoraOutputFormat.class, jobConf) ;
    }

    @Override
    public void sourceConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf jobConf) {
        // XXX Ugly, but temporary to know why is all this here...
        try {
            // Workaround to load Job configuration into JobConf.
            Job tmpGoraJob = new Job(jobConf);
            // Generics funnel
            if (this.query == null) {
                this.setQuery(this.getDataStore(jobConf).newQuery()) ;
            }
            this.genericSetInput(this.query, this.getDataStore(jobConf), this.keyClass, this.persistentClass, tmpGoraJob) ;
            // Copies the configuration set by "setInput()" into jobConf.
            this.mergeConfigurationFromTo(tmpGoraJob.getConfiguration(), jobConf) ; 
        } catch (Exception e) {
            throw new RuntimeException("Failed then configuring GoraInputFormat in the job",e) ;
        }
        DeprecatedInputFormatWrapper.setInputFormat(GoraInputFormat.class, jobConf, GoraDeprecatedInputFormatValueCopier.class) ;
    }
    
    /**
     * Generics funnel: Workaround for generics hassle. Executes GoraInputFormat.setInput() for a query and datastore on a job.
     * The query and datastore must be of the same generics types.
     * @param query 
     * @param dataStore
     * @param keyClass
     * @param persistentClass
     * @param job Job configuration
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    private <K1, V1 extends Persistent,
             K2, V2 extends Persistent,
             K, V extends Persistent>
    void genericSetInput(Query<K1,V1> query, DataStore<K2,V2> dataStore, Class<K> keyClass, Class<V> persistentClass, Job job) throws IOException {
        GoraInputFormat.setInput(job, (Query<K,V>)query, (DataStore<K,V>)dataStore, false) ;
    }

    /**
     * Merges configuration from the 'from' into the 'to'.
     * If a 'from' key exists in 'to', it is ignored.
     * If a 'from' key does not exists in 'to', it is added to 'to'.
     * 
     * Workaround used to merge Job configuration into JobConf
     * @param from
     * @param to
     */
    private void mergeConfigurationFromTo(Configuration from, Configuration to) {
        for(Entry<String,String> fromEntry: from) {
            if (to.get(fromEntry.getKey()) == null) {
                to.set(fromEntry.getKey(), fromEntry.getValue()) ;
            }
        }
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public boolean source(FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall) throws IOException {

        String key = null ;
        PersistentBase value = null ;
        
        if (!sourceCall.getInput().next(key, value)) {
            return false ;
        }
        
        Tuple tuple = sourceCall.getIncomingEntry().getTuple();
        tuple.clear();
        tuple.addString(key) ;
        tuple.add(value) ;
        
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void sink(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
        Fields fields = sinkCall.getOutgoingEntry().getFields() ;

        String key = (String)fields.get(0) ;
        PersistentBase persistent = (PersistentBase) fields.get(1) ;

        sinkCall.getOutput().collect(key, persistent) ;
    }

}
