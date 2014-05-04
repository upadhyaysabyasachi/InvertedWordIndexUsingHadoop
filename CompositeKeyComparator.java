package com.xml;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {
protected CompositeKeyComparator() {
super(CompositeKey.class, true);
}
@SuppressWarnings("rawtypes")
@Override
public int compare(WritableComparable w1, WritableComparable w2) {

CompositeKey key1 = (CompositeKey) w1;
CompositeKey key2 = (CompositeKey) w2;

// (first check on udid)
int compare = key1.gettag().split("\t")[0].compareTo(key2.gettag().split("\t")[0]);

if (compare == 0) {

	Double x= new Double(key2.getweight());
	
return x.compareTo(new Double(key1.getweight()));
}

return compare;
}
}