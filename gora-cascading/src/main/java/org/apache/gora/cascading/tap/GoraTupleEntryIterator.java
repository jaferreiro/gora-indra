package org.apache.gora.cascading.tap;

import java.io.IOException;

import cascading.scheme.Scheme;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

public class GoraTupleEntryIterator extends TupleEntryIterator {

    private Scheme scheme ;
    
    public GoraTupleEntryIterator(Scheme scheme) {
        this(Fields.ALL) ;
        this.scheme = scheme ;
    }
    
    public GoraTupleEntryIterator(Fields fields) {
        super(fields);
        // TODO Auto-generated constructor stub
    }

    @Override
    public boolean hasNext() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public TupleEntry next() {
        // TODO Auto-generated method stub
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
