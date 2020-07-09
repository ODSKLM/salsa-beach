package com.odsklm.salsabeach

import com.odsklm.salsabeach.types.ColumnDefs._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}

import scala.jdk.CollectionConverters._

object HBase extends LazyLogging {
  /**
    * Open a connection to HBase, execute the supplied function, and close the connection.
    *
    * @param f The function to execute
    */
  def withConnection[T](f: Connection => T): T = {
    val conn = createConnection()
    try {
      f(conn)
    } finally {
      conn.close()
    }
  }

  def createConnection(): Connection = {
    val config = ConfigFactory.load()
    val servers = config.getStringList("hbase.zookeeper.servers").asScala.toList
    val port = config.getInt("hbase.zookeeper.port")
    val znodeParent = config.getString("hbase.zookeeper.znode-parent")
    createConnection(servers, port, znodeParent)
  }

  /**
    * Open a connection to HBase, execute the supplied function, and close the connection.
    *
    * @param zkServers   The zookeeper quorum
    * @param port        The zookeeper port
    * @param znodeParent The Znode parent
    * @param f           The function to execute
    */
  def withConnection[T](zkServers: Seq[String], port: Int = 2181, znodeParent: String = "/hbase-unsecure")
                       (f: Connection => T): T = {
    val conn = createConnection(zkServers, port, znodeParent)
    try {
      f(conn)
    } finally {
      conn.close()
    }
  }

  def createConnection(zkServers: Seq[String], port: Int, znodeParent: String): Connection = {
    val hBaseConf = createHBaseConfig(zkServers, port, znodeParent)
    ConnectionFactory.createConnection(hBaseConf)
  }

  /**
    * Open an HBase table, execute the supplied function, and close the table.
    *
    * @param f    The function to execute
    * @param conn (implicit) The HBase connection
    */
  def withTable[T](tableName: String, namespace: String = "default")(f: Table => T)(implicit conn: Connection): T = {
    val table = openTable(namespace, tableName, conn)
    try {
      f(table)
    } finally {
      table.close()
    }
  }

  private def openTable(namespace: String, tableName: String, conn: Connection): Table = {
    conn.getTable(TableName.valueOf(namespace, tableName))
  }

  /**
    * Truncate (remove all records from) an HBase table.
    *
    * @param tableName      The table name
    * @param preserveSplits True if the splits should be preserved
    * @param conn           (implicit) The HBase connection
    */
  def truncate(tableName: String, preserveSplits: Boolean = true)(implicit conn: Connection): Unit = {
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
  def createOrUpdateTable(name: String, families: List[ColumnFamily], maxVersions: Int = 3, namespace: String = "default")(implicit conn: Connection): Unit = {
    val admin = conn.getAdmin

    val table = TableName.valueOf(namespace, name)
    // Create HBase table if it doesn't exist yet
    val td = if (admin.tableExists(table)) admin.getTableDescriptor(table)
    else new HTableDescriptor(table)

    // Add column families that are not yet present.
    val cfs = td.getFamilies.asScala.toSeq.map(cf => Bytes.toString(cf.getName))

    val addedFamilies = families
      .filter(f => !cfs.contains(f.columnFamily))
      .map(f => {
        td.addFamily(new HColumnDescriptor(f.columnFamily).setMaxVersions(maxVersions))
        f.columnFamily
      })
    if (addedFamilies.nonEmpty) {
      if (admin.tableExists(table)) {
        logger.info(s"Updating table $name with families [${addedFamilies.mkString(",")}]")
        admin.modifyTable(table, td)
      } else {
        logger.info(s"Creating table $name with families [${addedFamilies.mkString(",")}]")
        admin.createTable(td)
      }
    }
  }
}
