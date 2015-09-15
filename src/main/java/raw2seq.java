import java.io.File;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class raw2seq
{ 
		public static void main2( String[] args) throws IOException 
		{ 
				//input binary file stream
				File train_img_name= new File("./mnist/train-images.idx3-ubyte");
				FileInputStream train_img_steam= new FileInputStream (train_img_name);
				DataInputStream train_img_data= new DataInputStream(train_img_stream);
				byte []train_img_buffer=new byte[];
				train_img_data.read(train_img_buffer);
				//output sequence file name
				String output_seq_file= "./mnist/mnist.seq";
				Configuration conf = new Configuration();
				FileSystem fs = FileSystem.get(URI.create(uri), conf);
				Path path = new Path( output_seq_file);

				IntWritable key = new IntWritable();
				Text value = new Text();
				//create sequence file writer
				SequenceFile.Writer writer = null;
				try { 
						writer = SequenceFile.createWriter( fs, conf, path, key.getClass(), value.getClass());
						for (int i = 0; i < 100; i ++) { 
								key.set( 100 - i);
								value.set( DATA[ i % DATA.length]);
								System.out.println(i);
								writer.append( key, value); } 
				} finally 
				{ 
						IOUtils.closeStream(writer); 
				} 
		} 
}
