name := "SparkEtl"

version := "0.1"

scalaVersion := "2.11.0"

//unmanagedJars in Compile += file("C:\\Users\\mani\\IdeaProjects\\SparkEtl\\target\\spark-excel_2.11-0.12.0.jar")


// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.6"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.6"
// https://mvnrepository.com/artifact/com.crealytics/spark-excel
libraryDependencies += "com.crealytics" %% "spark-excel" % "0.10.2"

//libraryDependencies += "com.crealytics" %% "spark-excel" % "0.12.0" from "file:///Users/mani/IdeaProjects/SparkEtl/target/spark-excel_2.11-0.12.0.jar"

