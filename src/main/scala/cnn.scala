import org.apache.spark.{SparkConf, SparkContext}

object ConvolutionNeuralNetwork{
def main(args:Array[String])
{
	val conf = new SparkConf().setAppName("Convolution Neural Network")
 	val sc = new SparkContext(conf)
	println("hi!")	
}
}
