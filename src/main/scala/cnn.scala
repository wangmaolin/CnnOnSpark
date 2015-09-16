import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.hadoop.io._;

object ConvolutionNeuralNetwork{
	def main2(args:Array[String])
	{
		val conf = new SparkConf().setAppName("Convolution Neural Network")
		val sc = new SparkContext(conf)
		val train_data=sc.sequenceFile[IntWritable,BytesWritable]("mnist_train.seq")
	}
}
