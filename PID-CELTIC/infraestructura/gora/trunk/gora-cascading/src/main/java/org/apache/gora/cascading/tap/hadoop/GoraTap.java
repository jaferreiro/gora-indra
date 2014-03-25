package org.apache.gora.cascading.tap.hadoop;

import java.io.IOException;
import java.util.UUID;

import org.apache.gora.cascading.tap.local.GoraLocalTap;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.store.DataStore;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.util.GoraException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

@SuppressWarnings({ "rawtypes" })
public class GoraTap extends Tap<JobConf, RecordReader, OutputCollector> {

    public static final Logger LOG = LoggerFactory.getLogger(GoraLocalTap.class);

    // TODO Change this to something decent. Does this identifier affects optimizations of Taps usage?
    private final String tapId = UUID.randomUUID().toString() ;

    private Class<?> keyClass;
    private Class<? extends Persistent> persistentClass;

    // TODO transient?
    private DataStore<?, ? extends Persistent> dataStore ;

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
        super(scheme, sinkMode) ;
        this.keyClass = keyClass ;
        this.persistentClass = persistentClass ;
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
        return new HadoopTupleEntrySchemeIterator(flowProcess, this, input) ;
    }

    @Override
    public TupleEntryCollector openForWrite(FlowProcess<JobConf> flowProcess, OutputCollector output) throws IOException {
        GoraTupleEntrySchemeCollector goraTupleCollector = new GoraTupleEntrySchemeCollector(flowProcess, this) ;
        goraTupleCollector.prepare() ;
        return goraTupleCollector ;
    }

    @Override
    public void sourceConfInit(FlowProcess<JobConf> flowProcess, JobConf conf) {
        super.sourceConfInit(flowProcess, conf);
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> flowProcess, JobConf conf) {
        super.sinkConfInit(flowProcess, conf);
    }

}
