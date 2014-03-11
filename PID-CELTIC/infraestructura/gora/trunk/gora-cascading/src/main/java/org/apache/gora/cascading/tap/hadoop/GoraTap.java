package org.apache.gora.cascading.tap.hadoop;
/*package org.apache.gora.cascading.tap;

import java.io.IOException;
import java.util.UUID;
import java.util.Map.Entry;

import org.apache.gora.mapreduce.GoraInputFormat;
import org.apache.gora.mapreduce.GoraOutputFormat;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.query.Query;
import org.apache.gora.store.DataStore;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.util.GoraException;
import org.apache.gora.cascading.tap.GoraTupleEntrySchemeCollector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.Job;

import com.twitter.elephantbird.mapred.input.DeprecatedInputFormatWrapper;
import com.twitter.elephantbird.mapred.output.DeprecatedOutputFormatWrapper;

import cascading.flow.FlowProcess;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

@SuppressWarnings({ "rawtypes" })
public class GoraTap extends Tap<JobConf, RecordReader, OutputCollector> {

    // TODO Change this to something decent. Does this identifier affects optimizations of Taps usage?
    private final String tapId = UUID.randomUUID().toString() ;

    private Class<? extends Persistent> persistentClass;
    private Class<?> keyClass;

    //BORRAR
    GoraTupleEntryIterator borrar = null ;
    
    // TODO transient?
    private DataStore<?, ? extends Persistent> dataStore ;
    private Query<?, ? extends Persistent> query ;
    
    public GoraTap (Class<?> keyClass, Class<? extends Persistent> persistentClass, GoraScheme scheme) {
        this(keyClass, persistentClass, scheme, SinkMode.KEEP) ;
    }
    
    public GoraTap (Class<?> keyClass, Class<? extends Persistent> persistentClass, GoraScheme scheme, SinkMode sinkMode) {
        super(scheme, sinkMode) ;
        this.keyClass = keyClass ;
        this.persistentClass = persistentClass ;
    }

    *//**
     * Retrieves the datastore
     * @param conf (Optional) Needed the first time the datastore is retrieves for this scheme.
     *             In subsequent calls is ignored since the datastore is taken from cache.
     * @return
     * @throws GoraException
     *//*
    public DataStore<?, ? extends Persistent> getDataStore(JobConf conf) throws GoraException {
        if (this.dataStore == null) {
            this.dataStore = DataStoreFactory.getDataStore(this.keyClass, this.persistentClass, conf) ;
        }
        return this.dataStore ;
    }
    
    @Override
    public GoraScheme getScheme()
    {
        return (GoraScheme) super.getScheme() ;
    }
    
    @Override
    public String getIdentifier() {
        // TODO Change this to something decent. Does this identifier affects optimizations of Taps usage?
        return this.tapId ;
    }

    @Override
    public boolean createResource(JobConf conf) throws IOException {
        this.getDataStore(conf).createSchema() ;
        return true ; 
    }

    @Override
    public boolean deleteResource(JobConf conf) throws IOException {
        this.getDataStore(conf).deleteSchema() ;
        return true ;
    }

    @Override
    public boolean resourceExists(JobConf conf) throws IOException {
        return this.getDataStore(conf).schemaExists() ;
    }

    @Override
    public long getModifiedTime(JobConf conf) throws IOException {
        return System.currentTimeMillis(); // currently unable to find last mod time on a table
    }

    @Override
    public TupleEntryIterator openForRead(FlowProcess<JobConf> flowProcess, RecordReader input) throws IOException {
        // Devolver el TupleEntryIterator que a su vez devolverá instancias TupleEntry (cada registro),
        // que iterará sobre los resultados de un scan
        // @param input puede ser null y habrá que crear una instancia de GoraRecordReader
        
        // Internamente, TupleEntryIterator necesitará un TupleEntrySchemeIterator
        
        //return new GoraTupleEntryIterator(this.getScheme(), input) ;
        return new HadoopTupleEntrySchemeIterator(flowProcess, this, input) ;
    }

    @Override
    public TupleEntryCollector openForWrite(FlowProcess<JobConf> flowProcess, OutputCollector output) throws IOException {
        // Devolver un TupleEntryCollector que se recibirá instancias TupleEntry/Tuple para ir grabando.
        // @param output puede ser null y habrá que crear una instancia de GoraRecordWriter

        GoraTupleEntrySchemeCollector goraTupleCollector = new GoraTupleEntrySchemeCollector(flowProcess, this) ;
        goraTupleCollector.prepare() ;
        return goraTupleCollector ;
    }

    @Override
    public void sourceConfInit(FlowProcess<JobConf> flowProcess, JobConf conf) {
        // XXX Ugly, but temporary to know why is all this here...
        try {
            // Workaround to load Job configuration into JobConf.
            Job tmpGoraJob = new Job(conf);
            // Generics funnel
            if (this.query == null) {
                this.setQuery(this.getDataStore(conf).newQuery()) ;
            }
            this.genericSetInput(this.query, this.getDataStore(conf), this.keyClass, this.persistentClass, tmpGoraJob) ;
            // Copies the configuration set by "setInput()" into jobConf.
            this.mergeConfigurationFromTo(tmpGoraJob.getConfiguration(), conf) ; 
        } catch (Exception e) {
            throw new RuntimeException("Failed then configuring GoraInputFormat in the job",e) ;
        }
        DeprecatedInputFormatWrapper.setInputFormat(GoraInputFormat.class, conf, GoraDeprecatedInputFormatValueCopier.class) ;
        super.sourceConfInit(flowProcess, conf);
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> flowProcess, JobConf conf) {

        // do not delete if initialized from within a task
        if (isReplace() && conf.get("mapred.task.partition") == null) {
            try {
                this.deleteResource(conf);
            } catch (IOException e) {
                throw new RuntimeException("could not delete resource: " + e);
            }
        } else if (isUpdate()) {
            try {
                this.createResource(conf);
            } catch (IOException e) {
                throw new RuntimeException("error creating the sink resource !");
            }
        }

        // XXX Ugly, but temporary to know why is all this here...
        try {
            // Workaround to load Job configuration into JobConf.
            Job tmpGoraJob = new Job(conf);
            GoraOutputFormat.setOutput(tmpGoraJob, this.getDataStore(conf), true) ;
            // Copies the configuration set by "setOutput()" into jobConf.
            this.mergeConfigurationFromTo(tmpGoraJob.getConfiguration(), conf) ; 
        } catch (Exception e) {
            throw new RuntimeException("Failed then configuring GoraInputFormat in the job",e) ;
        }
        DeprecatedOutputFormatWrapper.setOutputFormat(GoraOutputFormat.class, conf) ;
        
        
        // TODO Auto-generated method stub
        super.sinkConfInit(flowProcess, conf);
    }

    @SuppressWarnings("unchecked")
    public void setQuery(Query query) {
        this.query = query ;
    }
    
    *//**
     * Generics funnel: Workaround for generics hassle. Executes GoraInputFormat.setInput() for a query and datastore on a job.
     * The query and datastore must be of the same generics types.
     * @param query 
     * @param dataStore
     * @param keyClass
     * @param persistentClass
     * @param job Job configuration
     * @throws IOException
     *//*
    @SuppressWarnings("unchecked")
    private <K1, V1 extends Persistent,
             K2, V2 extends Persistent,
             K, V extends Persistent>
    void genericSetInput(Query<K1,V1> query, DataStore<K2,V2> dataStore, Class<K> keyClass, Class<V> persistentClass, Job job) throws IOException {
        GoraInputFormat.setInput(job, (Query<K,V>)query, (DataStore<K,V>)dataStore, false) ;
    }
    
    *//**
     * Merges configuration from the 'from' into the 'to'.
     * If a 'from' key exists in 'to', it is ignored.
     * If a 'from' key does not exists in 'to', it is added to 'to'.
     * 
     * Workaround used to merge Job configuration into JobConf
     * @param from
     * @param to
     *//*
    private void mergeConfigurationFromTo(Configuration from, Configuration to) {
        for(Entry<String,String> fromEntry: from) {
            if (to.get(fromEntry.getKey()) == null) {
                to.set(fromEntry.getKey(), fromEntry.getValue()) ;
            }
        }
    }

}
*/