package org.apache.gora.cascading.tap.local;
/*package org.apache.gora.cascading.tap;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.scheme.Scheme;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

public class GoraLocalTupleEntryIterator extends TupleEntryIterator {

    public static final Logger LOG = LoggerFactory.getLogger(GoraTupleEntryIterator.class);
    
    private Scheme scheme ;
    private RecordReader recordReader ;

    private Object readKey ;
    private Object readValue ;
    private Object nextKey ;
    private Object nextValue ;
    
    public GoraLocalTupleEntryIterator(GoraLocalTap tap, Scheme scheme) {
        super(scheme.getSourceFields()) ;
        this.scheme = scheme ;
        this.recordReader =  tap.getDataStore(null);
    }
    
    @Override
    public boolean hasNext() {
        try {
            return this.recordReader.next(this.nextKey, this.nextValue) ;
        } catch (IOException e) {
            this.nextKey = null ;
            LOG.error("Error reading", e);
            return false ;
        }
    }

    @Override
    public TupleEntry next() {
        if (this.nextKey == null) {
            return new NoSuchElementException() ;
        }
        this.scheme.getSinkFields() ;
        // TODO transforamr Value => tupla
        return null;
    }

    @Override
    public void remove() {
    }

    @Override
    public void close() throws IOException {
        this.recordReader.close() ;
    }

}
*/