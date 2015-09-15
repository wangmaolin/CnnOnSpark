import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

object ConvolutionNeuralNetwork{
def main2(args:Array[String])
{
	val conf = new SparkConf().setAppName("Convolution Neural Network")
 	val sc = new SparkContext(conf)
	val logger = Logger.getLogger("testwml.foo");
	logger.setLevel(Level.INFO)
	logger.warn("warnning")
	logger.debug("warnning")
	println(Logger.getRootLogger())
}
}
