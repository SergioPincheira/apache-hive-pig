import os
import os.path
import sys
from shutil import copyfile, move
import subprocess
import urllib.request
import tarfile
import fileinput


def configure_file( filename, key, value ):
  with fileinput.FileInput(filename, inplace=True, backup='.bak') as file:
    for line in file:
      print( line.replace( key, value ), end = '' )

def hadoop_installer( version, src_dir, root_dir ):
  #hadoop_url = 'https://archive.apache.org/dist/hadoop/core/hadoop-%s/hadoop-%s.tar.gz' % (version, version)

  os.environ['HADOOP_HOME'] = os.path.join( root_dir, 'hadoop' )
  os.environ['HADOOP_PREFIX'] = os.environ['HADOOP_HOME']
  os.environ['HADOOP_CONF_DIR'] = os.path.join( os.environ['HADOOP_HOME'], 'etc/hadoop' )
  os.environ['HADOOP_MAPRED_HOME'] = os.environ['HADOOP_HOME']
  os.environ['HADOOP_COMMON_HOME'] = os.environ['HADOOP_HOME']
  os.environ['HADOOP_HDFS_HOME'] = os.environ['HADOOP_HOME']
  os.environ['YARN_HOME'] = os.environ['HADOOP_HOME']
  if 'PATH' in os.environ:
    os.environ['PATH'] += ':' + os.path.join( os.environ['HADOOP_HOME'], 'bin' )
  else:
    os.environ['PATH'] = os.path.join( os.environ['HADOOP_HOME'], 'bin' )

  #urllib.request.urlretrieve( hadoop_url, 'hadoop-%s.tar.gz' % version )

  fd = tarfile.open( os.path.join( src_dir, 'hadoop-%s.tar.gz' % version ), 'r:gz' )
  try:
    fd.extractall()
  finally:
    fd.close()

  subprocess.run( ['ln', '-s', 'hadoop-%s' % version, 'hadoop' ] )

  key = '</configuration>'
  value = '\
  <property>\n\
    <name>fs.defaultFS</name>\n\
    <value>hdfs://localhost:9000</value>\n\
  </property>\n\
</configuration>'
  configure_file( 'hadoop/etc/hadoop/core-site.xml', key, value )

  key = '</configuration>'
  value = '\
  <property>\n\
    <name>dfs.replication</name>\n\
    <value>1</value>\n\
  </property>\n\
  <property>\n\
    <name>dfs.permission</name>\n\
    <value>false</value>\n\
  </property>\n\
</configuration>'
  configure_file( 'hadoop/etc/hadoop/hdfs-site.xml', key, value )

  copyfile( 'hadoop/etc/hadoop/mapred-site.xml.template',
            'hadoop/etc/hadoop/mapred-site.xml' )
  key = '</configuration>'
  value = '\
  <property>\n\
    <name>mapreduce.framework.name</name>\n\
    <value>yarn</value>\n\
  </property>\n\
</configuration>'
  configure_file( 'hadoop/etc/hadoop/mapred-site.xml', key, value )

  key = '</configuration>'
  value = '\
  <property>\n\
    <name>yarn.nodemanager.aux-services</name>\n\
    <value>mapreduce_shuffle</value>\n\
  </property>\n\
  <property>\n\
    <name>yarn.nodemanager.auxservices.mapreduce.shuffle.class</name>\n\
    <value>org.apache.hadoop.mapred.ShuffleHandler</value>\n\
  </property>\n\
</configuration>'
  configure_file( 'hadoop/etc/hadoop/yarn-site.xml', key, value )

  key = 'export JAVA_HOME=${JAVA_HOME}'
  value = 'export JAVA_HOME=' + os.environ['JAVA_HOME']
  configure_file( 'hadoop/etc/hadoop/hadoop-env.sh', key, value )

  subprocess.run( ['hadoop/bin/hdfs', 'namenode', '-format'] )
  subprocess.run( ['hadoop/sbin/start-dfs.sh'] )
  subprocess.run( ['hadoop/sbin/start-yarn.sh'] )
  subprocess.run( ['hadoop/sbin/hadoop-daemon.sh', 'start', 'namenode'] )
  subprocess.run( ['hadoop/sbin/hadoop-daemon.sh', 'start', 'datanode'] )
  subprocess.run( ['hadoop/sbin/yarn-daemon.sh', 'start', 'resourcemanager'] )
  subprocess.run( ['hadoop/sbin/yarn-daemon.sh', 'start', 'nodemanager'] )
  subprocess.run( ['hadoop/sbin/mr-jobhistory-daemon.sh', 'start', 'historyserver'] )

  print( 'Active services:' )
  outtext = subprocess.run( ['jps'], stdout = subprocess.PIPE )
  print( outtext.stdout.decode( 'utf-8' ) )

  # Create home in hdfs for user root
  subprocess.run( ['hdfs', 'dfs', '-mkdir', '-p', '/user/root'] )

def hive_installer( version, src_dir, root_dir ):
  #hive_url = 'https://downloads.apache.org/hive/hive-%s/apache-hive-%s-bin.tar.gz' % (version, version)
  #derby_url = 'https://downloads.apache.org//db/derby/db-derby-10.14.2.0/db-derby-10.14.2.0-bin.tar.gz'
  #sqoop_url = 'https://downloads.apache.org/sqoop/1.4.7/sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz'
  #sqlconn_url = 'https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-8.0.20.tar.gz'

  if 'PATH' not in os.environ:
    os.environ['PATH'] = ''

  os.environ['HIVE_HOME'] = os.path.join( root_dir, 'apache-hive-bin' )
  os.environ['HIVE_CONF_DIR'] = os.path.join( os.environ['HIVE_HOME'], 'conf' )
  os.environ['DERBY_HOME'] = os.path.join( root_dir, 'db-derby-bin' )
  os.environ['PATH'] += ':' + os.path.join( os.environ['HIVE_HOME'], 'bin' )
  os.environ['PATH'] += ':' + os.path.join( os.environ['DERBY_HOME'], 'bin' )

  os.environ['SQOOP_HOME'] = os.path.join( root_dir, 'sqoop-bin' )
  os.environ['PATH'] += ':' + os.path.join( os.environ['SQOOP_HOME'], 'bin' )

  # These sets are just to avoid annoying warnings
  os.environ['HBASE_HOME'] = os.path.join( root_dir, 'hbase' )
  os.environ['HCAT_HOME'] = os.path.join( root_dir, 'hcat' )
  os.environ['ACCUMULO_HOME'] = os.path.join( root_dir, 'accumulo' )
  os.environ['ZOOKEEPER_HOME'] = os.path.join( root_dir, 'zookeeper' )
  os.makedirs( os.path.join( root_dir, 'hbase' ), exist_ok = True )
  os.makedirs( os.path.join( root_dir, 'hcat' ), exist_ok = True )
  os.makedirs( os.path.join( root_dir, 'accumulo' ), exist_ok = True )
  os.makedirs( os.path.join( root_dir, 'zookeeper' ), exist_ok = True )

  if 'CLASSPATH' not in os.environ:
    os.environ['CLASSPATH'] = ''
  os.environ['CLASSPATH'] += ':' + os.path.join( os.environ['HIVE_HOME'], 'lib/*:.' )
  os.environ['CLASSPATH'] += ':' + os.path.join( os.environ['DERBY_HOME'], 'lib/derby.jar' )
  os.environ['CLASSPATH'] += ':' + os.path.join( os.environ['DERBY_HOME'], 'lib/derbytools.jar' )

  #urllib.request.urlretrieve( hive_url, 'apache-hive-%s-bin.tar.gz' % version )
  fd = tarfile.open( os.path.join(src_dir, 'apache-hive-%s-bin.tar.gz' % version), 'r:gz' )
  try:
    fd.extractall()
  finally:
    fd.close()

  #urllib.request.urlretrieve( derby_url, 'db-derby-10.14.2.0-bin.tar.gz' )
  fd = tarfile.open( os.path.join(src_dir, 'db-derby-10.14.2.0-bin.tar.gz'), 'r:gz' )
  try:
    fd.extractall()
  finally:
    fd.close()

  #urllib.request.urlretrieve( sqoop_url, 'sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz' )
  fd = tarfile.open( os.path.join(src_dir, 'sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz'), 'r:gz' )
  try:
    fd.extractall()
  finally:
    fd.close()

  #urllib.request.urlretrieve( sqlconn_url, 'mysql-connector-java-8.0.20.tar.gz' )
  fd = tarfile.open( os.path.join(src_dir, 'mysql-connector-java-8.0.20.tar.gz'), 'r:gz' )
  try:
    fd.extractall()
  finally:
    fd.close()

  subprocess.run( ['ln', '-s', 'apache-hive-%s-bin' % version, 'apache-hive-bin' ] )
  subprocess.run( ['ln', '-s', 'db-derby-10.14.2.0-bin', 'db-derby-bin' ] )
  subprocess.run( ['ln', '-s', 'sqoop-1.4.7.bin__hadoop-2.6.0', 'sqoop-bin' ] )

  warehouse_dir = '/user/hive/warehouse'

  copyfile( 'apache-hive-bin/conf/hive-env.sh.template',
            'apache-hive-bin/conf/hive-env.sh' )

  text = list()
  text.append( 'export HADOOP_HOME=' + os.environ['HADOOP_HOME'] )
  text.append( 'export HADOOP_HEAPSIZE=512' )
  text.append( 'export HIVE_CONF_DIR=' + os.path.join( os.environ['HIVE_HOME'], 'conf' ) )
  with open( 'apache-hive-bin/conf/hive-env.sh', 'a+' ) as fd:
    for line in text:
      fd.write( line + '\n' )

  # Metastore on Derby

  os.makedirs( os.path.join( os.environ['DERBY_HOME'], 'data' ), exist_ok = True )

  text = '\
<configuration>\n\
  <property>\n\
    <name>javax.jdo.option.ConnectionURL</name>\n\
    <value>jdbc:derby:;databaseName=' + os.path.join( os.environ['HIVE_HOME'], 'metastore_db' ) + ';create=true</value>\n\
    <description>JDBC connect string for a JDBC metastore</description>\n\
  </property>\n\
  <property>\n\
    <name>hive.metastore.warehouse.dir</name>\n\
    <value>' + warehouse_dir + '</value>\n\
    <description>location of default database for the warehouse</description>\n\
  </property>\n\
  <property>\n\
    <name>javax.jdo.option.ConnectionDriverName</name>\n\
    <value>org.apache.derby.jdbc.EmbeddedDriver</value>\n\
    <description>Driver class name for a JDBC metastore</description>\n\
  </property>\n\
  <property>\n\
    <name>javax.jdo.PersistenceManagerFactoryClass</name>\n\
    <value>org.datanucleus.api.jdo.JDOPersistenceManagerFactory</value>\n\
    <description>class implementing the jdo persistence</description>\n\
  </property>\n\
  <property>\n\
    <name>hive.cli.print.header</name>\n\
    <value>true</value>\n\
    <description>Whether to print the names of the columns in query output</description>\n\
  </property>\n\
  <property>\n\
    <name>hive.support.concurrency</name>\n\
    <value>true</value>\n\
  </property>\n\
  <property>\n\
    <name>hive.enforce.bucketing</name>\n\
    <value>true</value>\n\
  </property>\n\
  <property>\n\
    <name>hive.exec.dynamic.partition.mode</name>\n\
    <value>nonstrict</value>\n\
  </property>\n\
  <property>\n\
    <name>hive.txn.manager</name>\n\
    <value>org.apache.hadoop.hive.ql.lockmgr.DbTxnManager</value>\n\
  </property>\n\
</configuration>\n'
  with open( 'apache-hive-bin/conf/hive-site.xml', 'w+' ) as fd:
    fd.write( text )

  text = list()
  text.append( 'javax.jdo.PersistenceManagerFactoryClass =' )
  text.append( 'org.jpox.PersistenceManagerFactoryImpl' )
  text.append( 'org.jpox.autoCreateSchema = false' )
  text.append( 'org.jpox.validateTables = false' )
  text.append( 'org.jpox.validateColumns = false' )
  text.append( 'org.jpox.validateConstraints = false' )
  text.append( 'org.jpox.storeManagerType = rdbms' )
  text.append( 'org.jpox.autoCreateSchema = true' )
  text.append( 'org.jpox.autoStartMechanismMode = checked' )
  text.append( 'org.jpox.transactionIsolation = read_committed' )
  text.append( 'javax.jdo.option.DetachAllOnCommit = true' )
  text.append( 'javax.jdo.option.NontransactionalRead = true' )
  text.append( 'javax.jdo.option.ConnectionDriverName = org.apache.derby.jdbc.ClientDriver' )
  text.append( 'javax.jdo.option.ConnectionURL = jdbc:derby:;databaseName=' + os.path.join( os.environ['HIVE_HOME'], 'metastore_db' ) + ';create=true' )
  text.append( 'javax.jdo.option.ConnectionUserName = APP' )
  text.append( 'javax.jdo.option.ConnectionPassword = mine' )
  with open( 'apache-hive-bin/conf/jpox.properties', 'a+' ) as fd:
    for line in text:
      fd.write( line + '\n' )

  subprocess.run( ['schematool', '-initSchema', '-dbType', 'derby'] )
  subprocess.run( ['hdfs', 'dfs', '-mkdir', '-p', '/tmp'] )
  subprocess.run( ['hdfs', 'dfs', '-mkdir', '-p', warehouse_dir] )
  subprocess.run( ['hdfs', 'dfs', '-chmod', 'g+w', '/tmp'] )
  subprocess.run( ['hdfs', 'dfs', '-chmod', 'g+w', warehouse_dir] )

  # Sqoop configuration

  copyfile( os.path.join( os.environ['HIVE_HOME'], 'lib/hive-common-%s.jar' % version ),
            os.path.join( os.environ['SQOOP_HOME'], 'lib/hive-common-%s.jar' % version ) )

  copyfile( os.path.join( os.environ['SQOOP_HOME'], 'conf/sqoop-env-template.sh' ),
            os.path.join( os.environ['SQOOP_HOME'], 'conf/sqoop-env.sh' ) )

  text = list()
  text.append( 'export HADOOP_COMMON_HOME=' + os.environ['HADOOP_COMMON_HOME'] )
  text.append( 'export HADOOP_MAPRED_HOME=' + os.environ['HADOOP_MAPRED_HOME'] )
  with open( 'sqoop-bin/conf/sqoop-env.sh', 'a+' ) as fd:
    for line in text:
      fd.write( line + '\n' )

  move( 'mysql-connector-java-8.0.20/mysql-connector-java-8.0.20.jar',
        os.path.join( os.environ['SQOOP_HOME'], 'lib' ) )

  subprocess.run( ['configure-sqoop'] )

  # MySQL installation and configuration

  subprocess.run( ['apt', 'install', 'mysql-server', 'mysql-client'] )
  #subprocess.run( ['mysql_secure_installation'] )

  key = 'bind-address\t\t= 127.0.0.1'
  value = 'bind-address\t\t= 0.0.0.0'
  configure_file( '/etc/mysql/mysql.conf.d/mysqld.cnf', key, value )

  subprocess.run( ['/etc/init.d/mysql', 'start'] )
  subprocess.run( ['mysql', '-e', 'CREATE DATABASE testdb;'] )
  subprocess.run( ['mysql', '-e', 'ALTER USER \'root\'@\'localhost\' IDENTIFIED WITH mysql_native_password BY \'password\';'] )
  subprocess.run( ['mysql', '-e', 'FLUSH PRIVILEGES;'] )

def pig_installer( version, src_dir, root_dir ):
  #pig_url = 'https://downloads.apache.org/pig/pig-%s/pig-%s.tar.gz' % (version, version)

  if 'PATH' not in os.environ:
    os.environ['PATH'] = ''

  if 'CLASSPATH' not in os.environ:
    os.environ['CLASSPATH'] = ''

  os.environ['PIG_HOME'] = os.path.join( root_dir, 'pig' )
  os.environ['PATH'] += ':' + os.path.join( os.environ['PIG_HOME'], 'bin' )
  os.environ['PIG_CLASSPATH'] = os.environ['HADOOP_CONF_DIR']
  os.environ['CLASSPATH'] += ':' + os.path.join( os.environ['HADOOP_HOME'], 'share/hadoop/common/hadoop-common-2.10.0.jar' )

  subprocess.run( ['apt', 'install', 'ant'] )

  #urllib.request.urlretrieve( pig_url, 'pig-%s.tar.gz' % version )
  fd = tarfile.open( os.path.join(src_dir, 'pig-%s.tar.gz' % version), 'r:gz' )
  try:
    fd.extractall()
  finally:
    fd.close()

  subprocess.run( ['ln', '-s', 'pig-%s' % version, 'pig' ] )

  os.makedirs( os.path.join( os.environ['PIG_HOME'], 'build/ivy/lib/Pig' ), exist_ok=True )
  subprocess.run( ['ant'], cwd = os.path.join( os.environ['PIG_HOME'], 'tutorial' ) )
  subprocess.run( ['tar', 'xzf', 'pigtutorial.tar.gz'], cwd = os.path.join( os.environ['PIG_HOME'], 'tutorial' ) )

  copyfile( 'pig/src/python/streaming/pig_util.py', 'pig_util.py' )


if __name__ == '__main__':

  subprocess.run( ['apt', 'update'] )
  subprocess.run( ['apt', 'install', 'openjdk-8-jdk'] )
  os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-8-openjdk-amd64'

  hadoop_installer( '2.10.1', '/content/drive/MyDrive/BigDataSw/', '/content' )
  hive_installer( '2.3.9', '/content/drive/MyDrive/BigDataSw/', '/content' )
  pig_installer( '0.17.0', '/content/drive/MyDrive/BigDataSw/', '/content' )


