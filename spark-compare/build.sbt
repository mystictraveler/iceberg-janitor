name := "iceberg-janitor-spark-compare"
version := "1.0.0"
scalaVersion := "2.12.18"

// Match EMR 7.7.0's Spark + Iceberg versions
val sparkVersion = "3.5.3"
val icebergVersion = "1.7.1"

libraryDependencies ++= Seq(
  "org.apache.spark"   %% "spark-sql"                % sparkVersion  % "provided",
  "org.apache.spark"   %% "spark-core"               % sparkVersion  % "provided",
  "org.apache.iceberg" %  "iceberg-spark-runtime-3.5_2.12" % icebergVersion,
)

// Shade the Iceberg runtime into the fat JAR so nothing needs to
// be downloaded at runtime. Spark + Hadoop are "provided" (EMR has them).
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", _*) => MergeStrategy.concat
  case PathList("META-INF", _*)             => MergeStrategy.discard
  case _                                    => MergeStrategy.first
}

assembly / assemblyJarName := "spark-compare-assembly.jar"
