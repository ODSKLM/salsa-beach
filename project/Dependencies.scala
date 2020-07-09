// scalafmt: { align.tokens.add = ["<-", "%%", "%", "="] }
import sbt._

object Dependencies {
  val hbaseVersion  = "2.2.0"
  val hadoopVersion = "2.8.5"

  lazy val dependencies = Seq(
    "org.apache.hbase"            % "hbase-client"    % hbaseVersion,
    "com.typesafe"                % "config"          % "1.3.0",
    "com.typesafe.scala-logging" %% "scala-logging"   % "3.9.2",
    "ch.qos.logback"              % "logback-classic" % "1.2.1" % Test,
    "org.codehaus.janino"         % "janino"          % "2.7.8" % Test
  )

  lazy val commonTestDeps = Seq(
    "org.scalatest" %% "scalatest" % "3.1.2" % "test"
  )

  lazy val hBaseTestDeps = Seq(
    "org.apache.hbase"  % "hbase-common"         % hbaseVersion  % Test,
    "org.apache.hbase"  % "hbase-server"         % hbaseVersion  % Test,
    "org.apache.hbase"  % "hbase-hadoop-compat"  % hbaseVersion  % Test,
    "org.apache.hbase"  % "hbase-hadoop2-compat" % hbaseVersion  % Test,
    "org.apache.hadoop" % "hadoop-hdfs"          % hadoopVersion % Test,
    "org.apache.hadoop" % "hadoop-common"        % hadoopVersion % Test,
    "org.apache.hbase"  % "hbase-testing-util"   % hbaseVersion  % Test
  )
}
