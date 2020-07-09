package com.odsklm.salsabeach.integrationtests.facilities

import java.io.File
import java.net.MalformedURLException
import java.nio.file.Files

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster
import org.apache.hadoop.hbase.{HBaseConfiguration, HBaseTestingUtility, HConstants}
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, Suite}
import org.scalatest.flatspec.AnyFlatSpec

@DoNotDiscover
trait HBaseEnabled extends BeforeAndAfterAll with LazyLogging { self: Suite =>

  private val workingDirectory = Files.createTempDirectory(null) + "/"

  /**
    * Test configuration for the mini-cluster
    */
  val hadoopConfiguration: Configuration = {
    val conf = new Configuration()
    conf.set("fs.default.name", "file:///")
    conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase-unsecure")

    try conf.set(HConstants.HBASE_DIR, new File(workingDirectory, "hbase").toURI.toURL.toString)
    catch {
      case mue: MalformedURLException => logger.error(mue.getMessage)
    }
    conf
  }

  private val zookeeperCluster = new MiniZooKeeperCluster(hadoopConfiguration)
  private val hbaseConf = HBaseConfiguration.create(hadoopConfiguration)
  private val hBaseUtility = new HBaseTestingUtility(hbaseConf)

  override def beforeAll {
    System.setProperty("config.resource", "reference-test.conf")
    super.beforeAll()
    try {
      zookeeperCluster.addClientPort(2182)
      zookeeperCluster.startup(new File(workingDirectory))
      logger.info("Zookeeper started.")

      hBaseUtility.setZkCluster(zookeeperCluster)
      hBaseUtility.startMiniCluster
      hBaseUtility.getHBaseCluster.startMaster
      logger.info("HBase test cluster started.")

    } catch {
      case e: Exception =>
        logger.error(e.getMessage)
        throw new RuntimeException(e)
    }
  }

  override def afterAll() {
    super.afterAll()
    hBaseUtility.getHBaseCluster.shutdown()
    hBaseUtility.shutdownMiniCluster()
    zookeeperCluster.shutdown()
  }
}
