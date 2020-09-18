package com.odsklm.salsabeach

import com.odsklm.salsabeach.types.ColumnDefs._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, Table, ConnectionFactory}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HColumnDescriptor, HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.security.UserGroupInformation
import scala.jdk.CollectionConverters._

object HBase extends LazyLogging {
  case class KerberosConfig(
      masterPrincipal: String,
      regionServerPrincipal: String,
      principal: String,
      keyTab: String)

  /**
    * Creates a HBase config
    *
    * @param zkServers      The Zookeeper servers
    * @param port           The port on which Zookeeper is listening
    * @param znodeParent    The Zookeeper node parent location
    * @param kerberosConfig Kerberos configuration
    * @return A Hadoop configuration object
    */
  private[salsabeach] def createHBaseConfig(
      zkServers: Seq[String],
      port: Int,
      znodeParent: String,
      kerberosConfig: Option[KerberosConfig] = None
    ): Configuration = {
    val c = HBaseConfiguration.create()
    c.set("hbase.zookeeper.property.clientPort", port.toString)
    c.set("hbase.zookeeper.quorum", zkServers.mkString(","))
    c.set("zookeeper.znode.parent", znodeParent)

    kerberosConfig.foreach { kerberosConfig =>
      c.set("hbase.cluster.distributed", "true")
      c.set("hbase.rpc.protection", "authentication")
      c.set("hbase.security.authentication", "kerberos")
      c.set("hadoop.security.authentication", "kerberos")
      c.set("hbase.regionserver.kerberos.principal", kerberosConfig.regionServerPrincipal)
      c.set("hbase.master.kerberos.principal", kerberosConfig.masterPrincipal)

      UserGroupInformation.setConfiguration(c)
    }
    c
  }

  /**
    * Create a connection to HBase using values provided in a config file on the classpath
    * Config file is expected to be in HOCON format
    *
    * @return HBase Connection object
    */
  def createConnection(): Connection = {
    val config = ConfigFactory.load()
    val servers = config.getStringList("hbase.zookeeper.servers").asScala.toList
    val port = config.getInt("hbase.zookeeper.port")
    val znodeParent = config.getString("hbase.zookeeper.znode-parent")
    val kerberosConfig =
      if (znodeParent == "/hbase-secure")
        Some(
          KerberosConfig(
            config.getString("hbase.masterKerberosPrincipal"),
            config.getString("hbase.regionserverKerberosPrincipal"),
            config.getString("hbase.principal"),
            config.getString("hbase.keyTab")
          )
        )
      else None
    createConnection(servers, port, znodeParent, kerberosConfig)
  }

  /**
    * Create a connection to HBase using the provided configuration values
    *
    * @param zkServers      List of Zookeeper servers that make up the Zooekeeper quorum
    * @param port           Port on which Zookeeper servers are reachable
    * @param znodeParent    Parent of ZNode where HBase stores its state in Zookeeper
    * @param kerberosConfig Optional Kerberos configuration
    * @return HBase Connection object
    */
  def createConnection(
      zkServers: Seq[String],
      port: Int,
      znodeParent: String,
      kerberosConfig: Option[KerberosConfig]
    ): Connection = {
    val hBaseConf = createHBaseConfig(zkServers, port, znodeParent, kerberosConfig)

    kerberosConfig.foreach { conf =>
      logger.info(s"Performing Kerberos login with principal ${conf.principal} from keytab ${conf.keyTab}")

      UserGroupInformation.loginUserFromKeytab(
        conf.principal,
        conf.keyTab
      )
    }

    ConnectionFactory.createConnection(hBaseConf)
  }

  /**
    * Open a connection to HBase, execute the supplied function, and close the connection.
    * Configuration will be taken from a config file on the classpath
    *
    * @param f The function to execute
    */
  def withConnection[T](f: Connection => T): T = {
    val conn = createConnection()
    try f(conn)
    finally conn.close()
  }

  /**
    * Open a connection to HBase, execute the supplied function with the connection passed in,
    * and close the connection. Connection will be closed regardless of successful execution of the function.
    *
    * @param zkServers      The zookeeper quorum
    * @param port           The zookeeper port
    * @param znodeParent    The Znode parent
    * @param kerberosConfig Optional Kerberos configuration when connecting to secured HBase cluster
    * @param f              The function to execute
    * @return Returned results of the executed function
    */
  def withConnection[T](
      zkServers: Seq[String],
      port: Int = 2181,
      znodeParent: String = "/hbase-unsecure",
      kerberosConfig: Option[KerberosConfig]
    )(f: Connection => T
    ): T = {
    val conn = createConnection(zkServers, port, znodeParent, kerberosConfig)
    try f(conn)
    finally conn.close()
  }

  /**
    * Open an HBase table, execute the supplied function, and close the table.
    *
    * @param f    The function to execute
    * @param conn (implicit) The HBase connection
    */
  def withTable[T](
      tableName: String,
      namespace: String = "default"
    )(f: Table => T
    )(implicit conn: Connection
    ): T = {
    val table = openTable(namespace, tableName, conn)
    try f(table)
    finally table.close()
  }

  private def openTable(namespace: String, tableName: String, conn: Connection): Table =
    conn.getTable(TableName.valueOf(namespace, tableName))

  /**
    * Truncate (remove all records from) an HBase table.
    *
    * @param tableName      The table name
    * @param preserveSplits True if the splits should be preserved
    * @param conn           (implicit) The HBase connection
    */
  def truncate(
      tableName: String,
      preserveSplits: Boolean = true
    )(implicit conn: Connection
    ): Unit = {
    val admin = conn.getAdmin
    val table = TableName.valueOf(tableName)
    admin.disableTable(table)
    admin.truncateTable(table, preserveSplits)
  }

  /**
    * Create or update an HBase table.
    *
    * @param name         The table name to create/update
    * @param families     The column families to create/add
    * @param maxVersions  Max versions for each column family
    * @param namespace    The namespace of the table to create/update
    * @param conn         (implicit) The HBase connection
    */
  def createOrUpdateTable(
      name: String,
      families: List[ColumnFamily],
      maxVersions: Int = 3,
      namespace: String = "default"
    )(implicit conn: Connection
    ): Unit = {
    val admin = conn.getAdmin

    val table = TableName.valueOf(namespace, name)
    // Create HBase table if it doesn't exist yet
    val td =
      if (admin.tableExists(table)) admin.getTableDescriptor(table)
      else new HTableDescriptor(table)

    // Add column families that are not yet present.
    val cfs = td.getFamilies.asScala.toSeq.map(cf => Bytes.toString(cf.getName))

    val addedFamilies = families
      .filter(f => !cfs.contains(f.columnFamily))
      .map { f =>
        td.addFamily(new HColumnDescriptor(f.columnFamily).setMaxVersions(maxVersions))
        f.columnFamily
      }
    if (addedFamilies.nonEmpty)
      if (admin.tableExists(table)) {
        logger.info(s"Updating table $name with families [${addedFamilies.mkString(",")}]")
        admin.modifyTable(table, td)
      } else {
        logger.info(s"Creating table $name with families [${addedFamilies.mkString(",")}]")
        admin.createTable(td)
      }
  }
}
