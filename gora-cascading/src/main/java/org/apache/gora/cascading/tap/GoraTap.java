package org.apache.gora.cascading.tap;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

@SuppressWarnings({ "rawtypes" })
public class GoraTap extends Tap<JobConf, RecordReader, OutputCollector> {

    public GoraTap (GoraScheme scheme) {
        this(scheme, SinkMode.UPDATE) ;
    }
    
    public GoraTap (GoraScheme scheme, SinkMode sinkMode) {
        super(scheme, sinkMode) ;
    }

    @Override
    public GoraScheme getScheme()
    {
        return (GoraScheme) super.getScheme() ;
    }
    
    @Override
    public String getIdentifier() {
        return this.getScheme().getPersistentClass().getName() ;
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
        this.getScheme().getDataStore(conf).createSchema() ;
        return true ; 
    }

    @Override
    public boolean deleteResource(JobConf conf) throws IOException {
        this.getScheme().getDataStore(conf).deleteSchema() ;
        return true ;
    }

    @Override
    public boolean resourceExists(JobConf conf) throws IOException {
        return this.getScheme().getDataStore(conf).schemaExists() ;
    }

    @Override
    public long getModifiedTime(JobConf conf) throws IOException {
        // Leer el tiempo de modificación del Scheme (llevar control de save en el Scheme)
        return 0;
    }

    @Override
    public TupleEntryIterator openForRead(FlowProcess<JobConf> flowProcess, RecordReader input) throws IOException {
        // Devolver el TupleEntryIterator que a su vez devolverá instancias TupleEntry (cada registro),
        // que iterará sobre los resultados de un scan
        // @param input puede ser null y habrá que crear una instancia de GoraRecordReader
        
        // Internamente, TupleEntryIterator necesitará un TupleEntrySchemeIterator
        return null;
    }
    
}
