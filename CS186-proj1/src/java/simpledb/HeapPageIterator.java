package simpledb;

import java.util.*;

class HeapPageIterator implements Iterator<Tuple> {

    private HeapPage page;
    private int slot_i;

    public HeapPageIterator(HeapPage page) {
        this.page = page;
        slot_i = 0;
    }

    public boolean hasNext() {
        for(int i = slot_i; i < page.numSlots; i++) {
            if(page.isSlotUsed(i)) {
                return true;
            }
        }
        return false;
    }

    public Tuple next() {
        if(!hasNext()) {
            throw new NoSuchElementException("no more tuples left");
        }
        while(!page.isSlotUsed(slot_i)) {
            slot_i++;
        }
        return page.tuples[slot_i];
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }
}

