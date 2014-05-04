package com.xml;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * This key is a composite key. The "actual"
 * key is the tag. The secondary sort will be performed against the weight.
 */
public class CompositeKey implements WritableComparable {

	private String tag;
	private double weight;

	public CompositeKey() {
	}

	public CompositeKey(String tag, double weight) {

		this.tag = tag;
		this.weight = weight;
	}

	@Override
	public String toString() {

		return (new StringBuilder()).append(tag).append(',').append(weight).toString();
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		tag = WritableUtils.readString(in);
		String temp=""; 
		temp = WritableUtils.readString(in);

		weight=Double.parseDouble(temp);

	}

	@Override
	public void write(DataOutput out) throws IOException {
		String temp="";

		temp=temp+weight;
		WritableUtils.writeString(out, tag);
		WritableUtils.writeString(out, temp);
		//weight=Double.parseDouble(temp);


	}

	public int compareTo(CompositeKey o) {

		int result = tag.compareTo(o.tag);
		if (0 == result) {

			Double w1=new Double(weight);
			Double w2=new Double(o.weight);

			result = w2.compareTo(w1);
		}
		return result;
	}

	/**
	 * Gets the tag.
	 *
	 * @return tag.
	 */
	public String gettag() {

		return tag;
	}

	public void settag(String tag) {

		this.tag = tag;
	}

	/**
	 * Gets the weight.
	 *
	 * @return weight
	 */
	public double getweight() {

		return weight;
	}

	public void setweight(double weight) {

		this.weight = weight;
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}
}

