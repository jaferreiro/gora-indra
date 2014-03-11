package org.apache.gora.cascading.tap.local;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;

import org.apache.gora.mapreduce.GoraInputFormat;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.query.impl.ResultBase;
import org.apache.gora.store.DataStore;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.util.GoraException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.Job;

import cascading.flow.FlowProcess;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntrySchemeCollector;
import cascading.tuple.TupleEntrySchemeIterator;

@SuppressWarnings({ "rawtypes" })
public class GoraLocalTap extends Tap<Properties, ResultBase, OutputCollector> {

    // TODO Change this to something decent. Does this identifier affects optimizations of Taps usage?
    private final String tapId = UUID.randomUUID().toString() ;

    private Class<?> keyClass;
    private Class<? extends Persistent> persistentClass;

    // TODO transient?
    private DataStore<?, ? extends Persistent> dataStore ;
    
    // HBase/HDFS configuration
    private JobConf jobConfiguration ;
    
    public GoraLocalTap (Class<?> keyClass, Class<? extends Persistent> persistentClass, GoraLocalScheme scheme) {
        this(keyClass, persistentClass, scheme, SinkMode.KEEP) ;
    }
    
    public GoraLocalTap (Class<?> keyClass, Class<? extends Persistent> persistentClass, GoraLocalScheme scheme, SinkMode sinkMode) {
        super(scheme, sinkMode) ;
        this.keyClass = keyClass ;
        this.persistentClass = persistentClass ;
        this.jobConfiguration = new JobConf(new Configuration()) ;
    }

    /**
     * Retrieves the datastore
     * @param _conf Ignored properties configuration, since the configuration needed for the dataStore is taken from gora.properties and from *-site.xml(Hadoop)
     * @return
     * @throws GoraException
     */
    public DataStore<?, ? extends Persistent> getDataStore(Properties _conf) throws GoraException {
        if (this.dataStore == null) {
            this.dataStore = DataStoreFactory.getDataStore(this.keyClass, this.persistentClass, this.jobConfiguration) ;
        }
        return this.dataStore ;
    }
    
    @Override
    public GoraLocalScheme getScheme()
    {
        return (GoraLocalScheme) super.getScheme() ;
    }
    
    @Override
    public String getIdentifier() {
        // TODO Change this to something decent. Does this identifier affects optimizations of Taps usage?
        return this.tapId ;
    }

    @Override
    public boolean createResource(Properties _conf) throws IOException {
        this.getDataStore(_conf).createSchema() ;
        return true ; 
    }

    @Override
    public boolean deleteResource(Properties _conf) throws IOException {
        this.getDataStore(_conf).deleteSchema() ;
        return true ;
    }

    @Override
    public boolean resourceExists(Properties _conf) throws IOException {
        return this.getDataStore(_conf).schemaExists() ;
    }

    @Override
    public long getModifiedTime(Properties _conf) throws IOException {
        return System.currentTimeMillis(); // currently unable to find last mod time on a table
    }

    @SuppressWarnings("unchecked")
    @Override
    public TupleEntryIterator openForRead(FlowProcess<Properties> flowProcess, ResultBase input) throws IOException {
        if (input != null) {
            return new TupleEntrySchemeIterator(flowProcess, this.getScheme(), input) ;
        }
        
        this.getScheme().
        
        
        return new TupleEntrySchemeIterator(flowProcess,this.getScheme(),) ;
    }

    @SuppressWarnings("unchecked")
    @Override
    public TupleEntryCollector openForWrite(FlowProcess<Properties> flowProcess, OutputCollector output) throws IOException {
        // Devolver un TupleEntryCollector que se recibirá instancias TupleEntry/Tuple para ir grabando.
        // @param output puede ser null y habrá que crear una instancia de GoraRecordWriter

        return new TupleEntrySchemeCollector(flowProcess, this.getScheme(), output) ;
    }

    @Override
    public void sourceConfInit(FlowProcess<Properties> flowProcess, Properties propsConf) {
        super.sourceConfInit(flowProcess, propsConf);
    }

    @Override
    public void sinkConfInit(FlowProcess<Properties> flowProcess, Properties conf) {
        super.sinkConfInit(flowProcess, conf);
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
    @SuppressWarnings({ "unchecked", "unused" })
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
    @SuppressWarnings("unused")
    private void mergeConfigurationFromTo(Configuration from, Configuration to) {
        for(Entry<String,String> fromEntry: from) {
            if (to.get(fromEntry.getKey()) == null) {
                to.set(fromEntry.getKey(), fromEntry.getValue()) ;
            }
        }
    }

}
