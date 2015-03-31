/*
 * Copyright 2015 Charith Ellawala
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.charithe.hbase;

import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HBaseMiniClusterBooter {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseMiniClusterBooter.class);

    private Configuration conf;

    private TestingServer zkServer;
    private MiniHBaseCluster hbaseCluster;

    public HBaseMiniClusterBooter(){
        this(HBaseConfiguration.create());
    }

    public HBaseMiniClusterBooter(Configuration conf){
        this.conf = conf;
    }

    public void start() throws HBaseMiniClusterStartupException {
        try {
            if(zkServer == null) {
                LOGGER.info("Starting a test Zookeeper cluster");
                zkServer = new TestingServer(true);
            }
            else{
                LOGGER.warn("Zookeeper is already started on port {}", zkServer.getPort());
            }

            if(hbaseCluster == null) {
                Configuration myConf = updateConfiguration(zkServer.getConnectString(), zkServer.getPort());

                HBaseTestingUtility testingUtility = new HBaseTestingUtility(myConf);
                testingUtility.cleanupTestDir();

                LOGGER.info("Starting mini HBase cluster");
                hbaseCluster = testingUtility.startMiniCluster();
            }
            else{
                LOGGER.warn("Mini HBase cluster is already started");
            }
        }
        catch(Exception e){
            LOGGER.error("Error during HBase mini cluster startup", e);
            throw new HBaseMiniClusterStartupException(e);
        }
    }

    public void stop(){
        if(hbaseCluster != null) {
            LOGGER.info("Stopping mini HBase cluster");
            try {
                hbaseCluster.shutdown();
            } catch (IOException e) {
                LOGGER.error("Caught exception during HBase shutdown", e);
            }
        }

        if(zkServer != null){
            LOGGER.info("Stopping test Zookeeper cluster");
            try {
                zkServer.close();
            } catch (IOException e) {
                LOGGER.error("Caught exception during Zookeeper shutdown", e);
            }
        }
    }

    private Configuration updateConfiguration(String zookeeperQuorum, int zkPort) throws IOException {
        LOGGER.debug("Updating configuration to use random ports and disable UIs");

        Configuration myConf = new Configuration(conf);
        myConf.setInt(HConstants.MASTER_PORT, getFreePort());
        myConf.setInt(HConstants.REGIONSERVER_PORT, getFreePort());
        myConf.setInt("hbase.master.info.port", -1);
        myConf.setInt("hbase.regionserver.info.port", -1);
        myConf.setBoolean("hbase.replication", false);

        myConf.setInt(HConstants.ZOOKEEPER_MAX_CLIENT_CNXNS, 80);
        myConf.set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum);
        myConf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zkPort);


        return myConf;
    }

    private int getFreePort() throws IOException {
        return InstanceSpec.getRandomPort();
    }


    public Configuration getHBaseConfiguration(){
        return hbaseCluster.getConfiguration();
    }

    public String getZookeeperQuorum() {
        return zkServer.getConnectString();
    }
}
