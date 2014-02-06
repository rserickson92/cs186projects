package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        Type fieldType;
        
        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return items.iterator();
    }

    private static final long serialVersionUID = 1L;
    private List<TDItem> items;
    private int tuple_size;
    private int hash_code;
    private void checkItemsBounds(int index) throws NoSuchElementException {
        if(index < 0 || index >= items.size()) {
            throw new NoSuchElementException("index out of bounds");
        }
    }
    private void hasAtLeastOneItem(Object[] array) {
        if(array.length < 1) { 
            throw new RuntimeException("array " + array + 
                                       "must have at least one entry");
        }
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */

    //initialize values here
    private TupleDesc() {
        tuple_size = 0;
        hash_code = 0;
    }

    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        this();
        hasAtLeastOneItem(typeAr);
        TDItem[] temp_items = new TDItem[typeAr.length];
        int field_hash;
        Type type;
        String field;
        for(int i = 0; i < temp_items.length; i++) {
            type = typeAr[i];
            field = fieldAr[i];
            temp_items[i] = new TDItem(type, field);
            tuple_size += type.getLen();
            if(field != null) { field_hash = field.hashCode(); }
            else { field_hash = 0; }
            hash_code += (i+1)*(type.hashCode() + field_hash);
        }
        items = Arrays.asList(temp_items);
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this();
        hasAtLeastOneItem(typeAr);
        TDItem[] temp_items = new TDItem[typeAr.length];
        Type type;
        for(int i = 0; i < temp_items.length; i++) {
            type = typeAr[i];
            temp_items[i] = new TDItem(typeAr[i], null);
            tuple_size += type.getLen();
            hash_code += (i+1)*type.hashCode();
        }
        items = Arrays.asList(temp_items);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return items.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        checkItemsBounds(i);
        return items.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        checkItemsBounds(i);
        return items.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        String field_name;
        for(int i = 0; i < numFields(); i++) {
            field_name = getFieldName(i);
            if((field_name == null && name == null) || 
               (field_name != null && field_name.equals(name))){ 
                return i;
            }
        }
        throw new NoSuchElementException("no matching field name found");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        return tuple_size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int num_td1 = td1.numFields();
        int num_td2 = td2.numFields();
        int len = num_td1 + num_td2;
        Type[] types = new Type[len];
        String[] fields = new String[len];
        for(int i = 0; i < len; i++) {
            if(i < num_td1) {
                types[i] = td1.getFieldType(i);
                fields[i] = td1.getFieldName(i);
            } else {
                types[i] = td2.getFieldType(i - num_td1);
                fields[i] = td2.getFieldName(i - num_td1);
            }
        }
        return new TupleDesc(types, fields);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        if(!(o instanceof TupleDesc) || 
           getSize() != ((TupleDesc) o).getSize()) { 
            return false; 
        } else {
            TupleDesc td = (TupleDesc) o;
            for(int i = 0; i < numFields(); i++) {
                if(!getFieldType(i).equals(td.getFieldType(i))) {
                    return false;
                }
            }
            return true;
        }
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        // throw new UnsupportedOperationException("unimplemented");
        return hash_code; //initialized in constructor since TupleDescs are immutable
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        int len = numFields();
        StringBuffer str = new StringBuffer(len);
        String index, separator = ", ";
        for(int i = 0; i < len; i++) {
            index = "[" + i + "]";
            str.append(getFieldType(i));
            str.append(index);
            str.append("(" + getFieldName(i) + index + ")");
            if(i != len-1) { str.append(separator); }
        }
        return str.toString();
    }
}
