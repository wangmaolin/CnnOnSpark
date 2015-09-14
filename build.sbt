lazy val root = ( project in file(".")).
	settings(
		name :="Convolution Neural Network",
		version :="1.0",
		libraryDependencies +="org.apache.spark" %% "spark-core" % "1.5.0"
)
