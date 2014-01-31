package org.apache.gora.cascading.tap;

import java.io.IOException;

import org.apache.hadoop.mapred.RecordReader;

import cascading.scheme.Scheme;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

public class GoraTupleEntryIterator extends TupleEntryIterator {

    private Scheme scheme ;
    private RecordReader recordReader ;

    private Object readKey ;
    private Object readValue ;
    private Object nextKey ;
    private Object nextValue ;
    
    public GoraTupleEntryIterator(Scheme scheme, RecordReader recordReader) {
        this(Fields.ALL) ;
        this.scheme = scheme ;
        this.recordReader = recordReader ;
    }
    
    public GoraTupleEntryIterator(Fields fields) {
        super(fields);
        // TODO Auto-generated constructor stub
    }

    @Override
    public boolean hasNext() {
        // TODO Auto-generated method stub
        return false ; //this.recordReader.next(this.nextKey, this.nextValue) ;
    }

    @Override
    public TupleEntry next() {
        return null;
    }

    @Override
    public void remove() {
        // TODO Auto-generated method stub
        return ;
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub
        return ;
    }

}
