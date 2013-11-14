package org.apache.gora.cascading.tap;

import java.io.IOException;

import org.apache.gora.mapreduce.GoraRecordReader;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.store.DataStore;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.util.GoraException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

import cascading.flow.FlowProcess;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

public class GoraTap extends Tap<JobConf, GoraRecordReader, OutputCollector> {

    private Class<? extends Persistent> persistentClass;
    private Class<?> keyClass;
    private DataStore<?, ? extends Persistent> dataStore ;
    
    
    public GoraTap (Class<?> keyClass, Class<? extends Persistent> persistentClass, GoraScheme scheme) {
        this(keyClass, persistentClass, scheme, SinkMode.UPDATE) ;
    }
    
    public GoraTap (Class<?>keyClass, Class<? extends Persistent> persistentClass, GoraScheme scheme, SinkMode sinkMode) {
        super(scheme,sinkMode) ;
        this.keyClass = keyClass ;
        this.persistentClass = persistentClass ;
    }
    
    public Class<?> getKeyClass() {
        return this.keyClass ;
    }
    
    public Class<? extends Persistent> getPersistentClass() {
        return this.persistentClass ;
    }
    
    @Override
    public String getIdentifier() {
        return this.persistentClass.getName() ;
    }

    protected DataStore<?, ?> getDataStore(JobConf conf) throws GoraException {
        if (this.dataStore == null) {
            this.dataStore = DataStoreFactory.getDataStore(this.keyClass, this.persistentClass, conf) ;
        }
        return this.dataStore ;
    }
    
    @Override
    public TupleEntryIterator openForRead(FlowProcess<JobConf> flowProcess, GoraRecordReader input) throws IOException {
        // Devovler el TupleEntryIterator que a su vez devolverá instancias TupleEntry (cada registro),
        // que iterará sobre los resultados de un scan
        // @param input puede ser null y habrá que crear una instancia de GoraRecordReader
        
        // Internamente, TupleEntryIterator necesitará un TupleEntrySchemeIterator
        return null;
    }

    @Override
    public TupleEntryCollector openForWrite(FlowProcess<JobConf> flowProcess, OutputCollector output) throws IOException {
        // Devolver un TupleEntryCollector que se recibirá instancias TupleEntry/Tuple para ir grabando.
        // @param otuput puede ser null y habrá que crear una instancia de GoraRecordWriter

        // Internamente, TupleEntryIterator necesitará un TupleEntrySchemeIterator
        return null;
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
        // Leer el tiempo de modificación del Scheme (llevar control de save en el Scheme)
        return 0;
    }
    
}
