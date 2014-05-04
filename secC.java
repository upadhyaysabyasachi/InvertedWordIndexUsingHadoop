package com.xml;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.StringTokenizer;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//XMLInputFactory;
import org.apache.log4j.Logger;


public class secC
{

        public static class XmlInputFormat1 extends TextInputFormat {

        public static final String START_TAG_KEY = "xmlinput.start";
        public static final String END_TAG_KEY = "xmlinput.end";


        public RecordReader<LongWritable, Text> createRecordReader(
                InputSplit split, TaskAttemptContext context) {
            return new XmlRecordReader();
        }

        /**
         * XMLRecordReader class to read through a given xml document to output
         * xml blocks as records as specified by the start tag and end tag
         *
         */
        
        public static class XmlRecordReader extends
                RecordReader<LongWritable, Text> {
            private byte[] startTag;
            private byte[] endTag;
            private long start;
            private long end;
            private FSDataInputStream fsin;
            private DataOutputBuffer buffer = new DataOutputBuffer();

            private LongWritable key = new LongWritable();
            private Text value = new Text();
                @Override
            public void initialize(InputSplit split, TaskAttemptContext context)
                    throws IOException, InterruptedException {
                Configuration conf = context.getConfiguration();
                startTag = conf.get(START_TAG_KEY).getBytes("utf-8");
                endTag = conf.get(END_TAG_KEY).getBytes("utf-8");
                FileSplit fileSplit = (FileSplit) split;

                // open the file and seek to the start of the split
                start = fileSplit.getStart();
                end = start + fileSplit.getLength();
                Path file = fileSplit.getPath();
                FileSystem fs = file.getFileSystem(conf);
                fsin = fs.open(fileSplit.getPath());
                fsin.seek(start);

            }
        @Override
            public boolean nextKeyValue() throws IOException,
                    InterruptedException {
                if (fsin.getPos() < end) {
                    if (readUntilMatch(startTag, false)) {
                        try {
                            buffer.write(startTag);
                            if (readUntilMatch(endTag, true)) {
                                key.set(fsin.getPos());
                                value.set(buffer.getData(), 0,
                                        buffer.getLength());
                                return true;
                            }
                        } finally {
                            buffer.reset();
                        }
                    }
                }
                return false;
            }
        @Override
           public LongWritable getCurrentKey() throws IOException,
                    InterruptedException {
                return key;
            }

        @Override
            public Text getCurrentValue() throws IOException,
                    InterruptedException {
                return value;
            }
        @Override
            public void close() throws IOException {
                fsin.close();
            }
        @Override
            public float getProgress() throws IOException {
                return (fsin.getPos() - start) / (float) (end - start);
            }

            private boolean readUntilMatch(byte[] match, boolean withinBlock)
                    throws IOException {
                int i = 0;
                while (true) {
                    int b = fsin.read();
                    // end of file:
                    if (b == -1)
                        return false;
                    // save to buffer:
                    if (withinBlock)
                        buffer.write(b);
                    // check if we're matching:
                    if (b == match[i]) {
                        i++;
                        if (i >= match.length)
                            return true;
                    } else
                        i = 0;
                    // see if we've passed the stop point:
                    if (!withinBlock && i == 0 && fsin.getPos() >= end)
                        return false;
                }
            }
        }
    }


        public static class Map extends Mapper<LongWritable, Text,
    CompositeKey, Text> {
  @Override
  protected void map(LongWritable key, Text value,
                     Context context)
      throws
      IOException, InterruptedException {
    String document = value.toString();
    System.out.println("�" + document + "�");
    
    Logger log=Logger.getLogger("info");
    CompositeKey temp= new CompositeKey();
    
    
        try {
      XMLStreamReader reader =
          XMLInputFactory.newInstance().createXMLStreamReader(new
              ByteArrayInputStream(document.getBytes()));
      
     
      
      String url = "";
      String filetype = "";
      String filename="";
      String users="";
      String tags="";
      String tag="";
      String name="";
      String weight="";
      String nameWeightPair="";
      
      String currentElement = "";
      while (reader.hasNext()) {
        int code = reader.next();
        switch (code) {
          case XMLStreamConstants.START_ELEMENT: //START_ELEMENT:
            currentElement = reader.getLocalName();
            ///log.info("*******"+ currentElement+"**********");
           System.out.println("*******"+currentElement);
            break;
          case XMLStreamConstants.CHARACTERS:  //CHARACTERS:
            if (currentElement.equalsIgnoreCase("url")) {
              url += reader.getText().trim();
              //log.info("*******"+ currentElement+":"+url+"**********");
             // System.out.println("111111"+currentElement+":"+url);
            } else if (currentElement.equalsIgnoreCase("filetype")) {
              filetype += reader.getText().trim();
             // log.info("*******"+ currentElement+":"+filetype+"**********");
             // System.out.println("222222222222222"+currentElement+":"+filetype);
            }
            else if (currentElement.equalsIgnoreCase("filename")) {
                filename += reader.getText().trim();
                //System.out.println(propertyValue);
              }
            
            else if (currentElement.equalsIgnoreCase("users")) {
                users += reader.getText().trim();
                //System.out.println(propertyValue);
              }
            else if (currentElement.equalsIgnoreCase("name")) {
                name += reader.getText().trim()+" ";
                //System.out.println(propertyValue);
              }
            else if (currentElement.equalsIgnoreCase("weight")) {
                weight +=reader.getText().trim()+" ";
                //System.out.println(propertyValue);
              }
            break;
            
           // default:
            	//continue;
        }
      }
      
      
      StringTokenizer token1=new StringTokenizer(name);
      StringTokenizer token2=new StringTokenizer(weight);
      
      while(token1.hasMoreTokens())
      {
    	  temp.settag(token1.nextToken()+ "\t" + url ) ;
    	  temp.setweight(Double.parseDouble(token2.nextToken()));
    	  
    	  System.out.println(temp.gettag());
    	  context.write(temp, new Text(""));
      }
      
      
      
      
     
      reader.close();
      
     
    }
        catch(Exception e){
                throw new IOException(e);

                }

  }
}
public static class Reduce
    extends Reducer<CompositeKey, Text, Text, Text> {

  @Override
 protected void setup(
     Context context)
     throws IOException, InterruptedException {
  context.write(new Text(""), null);
  }

    @Override protected void cleanup(
      Context context)
      throws IOException, InterruptedException {
    context.write(new Text(""), null);
  }

  private Text outputKey = new Text();
  public void reduce(CompositeKey key, Iterable<Text> values,
                     Context context)
      throws IOException, InterruptedException {
    for (Text value : values) {
      outputKey.set(constructPropertyXml(key, value));
      context.write(outputKey, null);
    }
  }

  public static String constructPropertyXml(CompositeKey name, Text value) {
    StringBuilder sb = new StringBuilder();
    
    sb.append(name.gettag()).append("\t").append(name.getweight());
    return sb.toString();
  }
}



        public static void main(String[] args) throws Exception
        {
                Configuration conf = new Configuration();

                conf.set("xmlinput.start", "<document>");
                conf.set("xmlinput.end", "</document>");
                Job job = new Job(conf);
                
                job.setJarByClass(sec.class);
                
             

                job.setMapperClass(sec.Map.class);
                job.setReducerClass(sec.Reduce.class);

                job.setMapOutputKeyClass(CompositeKey.class);
                job.setMapOutputValueClass(Text.class);
                
                
                job.setPartitionerClass(ActualKeyPartitioner.class);
                job.setGroupingComparatorClass(ActualKeyGroupingComparator.class);
                job.setSortComparatorClass(CompositeKeyComparator.class);
                
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                
                job.setNumReduceTasks(27);
                
                job.setInputFormatClass(XmlInputFormat1.class);
                job.setOutputFormatClass(TextOutputFormat.class);

                FileInputFormat.addInputPath(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));

                job.waitForCompletion(true);
        }
}

