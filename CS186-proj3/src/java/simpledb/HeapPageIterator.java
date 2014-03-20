package simpledb;

import java.util.*;

class HeapPageIterator implements Iterator<Tuple> {
    /** An iterator specifically for iterating through Tuples on
     *  a heap page. Accesses package-protected fields in HeapPages.
     */

    private HeapPage page;
    private int slot_i;
    private Iterator<Tuple> tuples;

    public HeapPageIterator(HeapPage page) {
        this.page = page;
        Tuple t;
        ArrayList<Tuple> l = new ArrayList<Tuple>();
        for(int i = 0; i < page.numSlots; i++) {
            if(page.isSlotUsed(i)) {
                l.add(page.tuples[i]);
            }
        }
        tuples = l.iterator();
    }

    public boolean hasNext() {
        return tuples.hasNext();
    }

    public Tuple next() {
        if(!tuples.hasNext()) {
            throw new NoSuchElementException("no more tuples left");
        }
        return tuples.next();
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }
}
