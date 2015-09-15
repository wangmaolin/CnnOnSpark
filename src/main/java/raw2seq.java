import java.io.BufferedInputStream;  
import java.io.FileInputStream;  
import java.io.DataInputStream;  
import java.io.IOException;  
import java.io.InputStream;  
import java.io.File;  
import java.net.URI;  

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.IOUtils;  
import org.apache.hadoop.io.SequenceFile;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.BytesWritable;

public class raw2seq
{  
	public static void main(String[] args) throws IOException 
	{  
		String seqFsUrl;
		String image_file_name("./mnist/train-images.idx3-ubyte");
		String label_file_name("./mnist/train-images.idx3-ubyte");
		//input stream
		File input_image_file= new File(image_file_name);
		File input_label_file= new File(label_file_name);

		FileInputStream image_stream= new FileInputStream(input_image_file);
		FileInputStream label_stream= new FileInputStream(input_label_file);

		DataInputStream image_data= new DataInputStream(image_stream);
		DataInputStream label_data= new DataInputStream(label_stream);

		int label_number=(int)input_label_file.length();
		int sample_number=(int)input_label_file.length();

		//output sequence file
		seqFsUrl="mnist_train.seq";
		Configuration conf = new Configuration();  
		FileSystem fs = FileSystem.get(URI.create(seqFsUrl),conf);  
		Path seqPath = new Path(seqFsUrl);  
		SequenceFile.Writer writer = null;  
		
		//input signal parts and redundancy
		byte[] image= new byte[784];
		byte[] label= new byte[1];
		
		Text key = new Text();
		String key_str;
		BytesWritable signal = new BytesWritable();
		try 
		{	//返回一个SequenceFile.Writer实例 需要数据流和path对象 将数据写入了path对象  
			writer = SequenceFile.createWriter(fs, conf, seqPath,Text.class, BytesWritable.class);  
			int i=0;
			int value_number=Integer.parseInt(args[2]);
			//while(i<62063)
			while(i<value_number)
			{
				for(int j=0;j<50;j++)
				{
					key_array[j]=(char)signalstream.readByte();
					if(key_array[j]==0)
						break;
				}
				for(int j=0;j<2;j++)
				{
					byte temp = signalstream.readByte();
				}
				for(int j=0;j<5760;j++)
				{
					b[j]=signalstream.readByte();
					if(i==0)	
					{
						match[j]=b[j];
					}
				}
				for(int j=5760;j<11520;j++)
				{
					b[j]=match[j-5760];
				}

				key_str=new String(key_array);
				key.set(key_str);
				//System.out.println("key:"+key_str);
				if(i%1000==0)
				{
					System.out.println("generate 1000 key value pairs");
				}

				signal.set(b,0,11520);

				writer.append(key, signal);
				i++;
			}
		}
		finally 
		{  
			IOUtils.closeStream(writer);  
		}  
	}  
}  
