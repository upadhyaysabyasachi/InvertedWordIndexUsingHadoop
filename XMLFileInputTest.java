package com.xml;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.junit.Assert;
import org.junit.Test;
 
public class XMLFileInputTest {
    @Test
    public void testXmlInputFormat() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.getLocal(conf);
 
        Path path = new Path("test.xml");
        FSDataOutputStream out = fs.create(path, true);
        out.writeBytes("<details>\n");
        out.writeBytes("  <stud roll=\"15\">\n");
        out.writeBytes("    <name>adv</name>\n");
        out.writeBytes("    <school>a</school>\n");
        out.writeBytes("  </stud>\n");
        out.writeBytes("  <stud roll=\"26\">\n");
        out.writeBytes("    <name>aghy</name>\n");
        out.writeBytes("    <school>a</school>\n");
        out.writeBytes("  </stud>\n");
        out.writeBytes("</details>\n");
 
        out.close();
 
        conf.set(XmlInputFormat.START_TAG_KEY, "<stud> ");
        conf.set(XmlInputFormat.END_TAG_KEY, "</stud>");
 
        XmlInputFormat inputFormat = new XmlInputFormat();
        FileSplit fileSplit = new FileSplit(path, 0, fs.getFileStatus(path)
                .getLen(), new String[] {});
        TaskAttemptContext context = new TaskAttemptContext(conf,
                new TaskAttemptID());
        RecordReader<LongWritable, Text> reader = inputFormat
                .createRecordReader(fileSplit, context);
        reader.initialize(fileSplit, context);
 
        Assert.assertTrue(reader.nextKeyValue());
        Assert.assertEquals(
                new Text(
                        "<stud roll=\"15\">\n    <name>adv</name>\n    <school>a</school>\n  </stud>"),
                reader.getCurrentValue());
        Assert.assertTrue(reader.nextKeyValue());
        Assert.assertEquals(
                new Text(
                        "<stud roll=\"26\">\n    <name>aghy</name>\n    <school>a</school>\n  </stud>"),
                reader.getCurrentValue());
        Assert.assertFalse(reader.nextKeyValue());
    }
 
    /**
     * Reads records that are delimited by a specifc begin/end tag.
     * 
     * Adapted from Mahout: https://github.com/apache/mahout/blob/
     * ad84344e4055b1e6adff5779339a33fa29e1265d
     * /examples/src/main/java/org/apache
     * /mahout/classifier/bayes/XmlInputFormat.java
     */
    public static class XmlInputFormat extends TextInputFormat {
 
        public static final String START_TAG_KEY = "xmlinput.start";
        public static final String END_TAG_KEY = "xmlinput.end";
 
        @Override
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
}