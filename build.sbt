import AssemblyKeys._
name := "creeper"

version := "0.1.0"

crossScalaVersions := Seq("2.10.6", "2.11.8")

assemblySettings

assemblyOption in assembly ~= { _.copy(includeScala = false) }

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-language:postfixOps"
)

resolvers ++= Seq(
  "Cloudera repos" at "https://repository.cloudera.com/artifactory/cloudera-repos",
  "Cloudera releases" at "https://repository.cloudera.com/artifactory/libs-release"
)
resolvers += "Local Maven Repository" at "file:///Users/jerry.li/.m2/repository"

val sparkVersion = "1.6.3"
val hbaseVersion = "1.2.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.hbase" % "hbase-common" % hbaseVersion % "provided",
  "org.apache.hbase" % "hbase-client" % hbaseVersion % "provided"
)
libraryDependencies += "eu.unicredit" %% "hbase-rdd" % "0.8.0"
libraryDependencies += "com.hankcs" % "hanlp" % "portable-1.3.2"
libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.21"