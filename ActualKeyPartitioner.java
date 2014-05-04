package com.xml;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

public class ActualKeyPartitioner extends
		Partitioner<CompositeKey, Text> {

	@Override
	public int getPartition(CompositeKey key, Text value,
			int numReduceTasks) {
		
		
		int hash = key.gettag().split("\t")[0].toString().toUpperCase().charAt(0)-'A';
		
		System.out.println("hash: "+hash);
		//int partition = hash % numReduceTasks;
		//System.out.println("par: "+partition);
		//return partition;
		if(hash>=0 && hash<=25)
			return hash;
		else 
			return 26;
		

	}
}