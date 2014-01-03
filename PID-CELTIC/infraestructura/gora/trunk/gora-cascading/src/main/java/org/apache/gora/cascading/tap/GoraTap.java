package org.apache.gora.cascading.tap;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

@SuppressWarnings({ "rawtypes" })
public class GoraTap extends Tap<Object, RecordReader, OutputCollector> {

    // TODO Change this to something decent. Does this identifier affects optimizations of Taps usage?
    private final String tapId = UUID.randomUUID().toString() ;
    
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
        // TODO Change this to something decent. Does this identifier affects optimizations of Taps usage?
        return this.tapId ;
    }

    @Override
    public boolean createResource(Object conf) throws IOException {
        this.getScheme().getDataStore((JobConf)conf).createSchema() ;
        return true ; 
    }

    @Override
    public boolean deleteResource(Object conf) throws IOException {
        this.getScheme().getDataStore((JobConf)conf).deleteSchema() ;
        return true ;
    }

    @Override
    public boolean resourceExists(Object conf) throws IOException {
        return this.getScheme().getDataStore((JobConf)conf).schemaExists() ;
    }

    @Override
    public long getModifiedTime(Object conf) throws IOException {
        // Leer el tiempo de modificación del Scheme (llevar control de save en el Scheme)
        return 0;
    }
    
    @Override
    public TupleEntryCollector openForWrite(FlowProcess<Object> flowProcess, OutputCollector output) throws IOException {
        // Devolver un TupleEntryCollector que se recibirá instancias TupleEntry/Tuple para ir grabando.
        // @param output puede ser null y habrá que crear una instancia de GoraRecordWriter

        return new GoraTupleEntryCollector(this.getScheme()) ;
    }

    @Override
    public TupleEntryIterator openForRead(FlowProcess<Object> flowProcess, RecordReader input) throws IOException {
        // Devolver el TupleEntryIterator que a su vez devolverá instancias TupleEntry (cada registro),
        // que iterará sobre los resultados de un scan
        // @param input puede ser null y habrá que crear una instancia de GoraRecordReader
        
        // Internamente, TupleEntryIterator necesitará un TupleEntrySchemeIterator
        
        return new GoraTupleEntryIterator(this.getScheme()) ;
    }
    
}
