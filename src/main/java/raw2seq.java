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
import org.apache.hadoop.io.IntWritable;

public class raw2seq
{  
	//public static void gen_seqfile(String[] args) throws IOException 
	public static void main(String[] args) throws IOException 
	{  
		String seqFsUrl;
		//switch for write train or test data
		boolean train_or_test=true;
		//boolean train_or_test=false;
		//input stream
		File input_image_file=null;
		File input_label_file=null; 
		if(train_or_test)
		{
			input_image_file= new File("./mnist/train-images.idx3-ubyte");
			input_label_file= new File("./mnist/train-labels.idx1-ubyte");
		}
		else
		{
			input_image_file= new File("./mnist/t10k-images.idx3-ubyte");
			input_label_file= new File("./mnist/t10k-labels.idx1-ubyte");
		}

		FileInputStream image_stream= new FileInputStream(input_image_file);
		FileInputStream label_stream= new FileInputStream(input_label_file);

		DataInputStream image_data= new DataInputStream(image_stream);
		DataInputStream label_data= new DataInputStream(label_stream);

		int label_number=(int)input_label_file.length();
		int sample_number=(int)input_label_file.length();
		//mnist head information
		int image_magic_number;
		int image_item_number;
		int image_col_number;
		int image_row_number;

		int label_magic_number;
		int label_item_number;

		//output sequence file
		if(train_or_test)
		{
			seqFsUrl="mnist_train.seq";
		}
		else
		{
			seqFsUrl="mnist_test.seq";
		}

		Configuration conf = new Configuration();  
		FileSystem fs = FileSystem.get(URI.create(seqFsUrl),conf);  
		Path seqPath = new Path(seqFsUrl);  
		SequenceFile.Writer writer = null;  

		//Mnist Image and Label buffer
		byte[] image= new byte[784];
		//byte label;
		byte[] label= new byte[1];

		//IntWritable key= new IntWritable();
		BytesWritable key= new BytesWritable();
		BytesWritable value= new BytesWritable();
		try 
		{	//return a SequenceFile.Writer instance, need dataflow and path object
			writer = SequenceFile.createWriter(fs, conf, seqPath,BytesWritable.class, BytesWritable.class);  
			label_magic_number=label_data.readInt();
			label_item_number=label_data.readInt();

			image_magic_number=image_data.readInt(); 
			image_item_number=image_data.readInt();
			image_col_number=image_data.readInt();
			image_row_number=image_data.readInt();

			System.out.println(label_item_number);
			System.out.println(image_item_number);
			System.out.println(image_col_number);
			System.out.println(image_row_number);
			for(int i=1;i<=label_item_number;i++)
			{
				//label=label_data.readByte();
				//key.set(i*10+(int)label);
				label[0]=label_data.readByte();
				key.set(label,0,1);

				for(int j=0;j<784;j++)
				{
					image[j]=image_data.readByte();
				}
				value.set(image,0,784);

				writer.append(key,value);
				if(i%100==0)
				{
					System.out.println(i);
				}
			}
		}
		finally 
		{  
			IOUtils.closeStream(writer);  
		}  
	}  
}  
