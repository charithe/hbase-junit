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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;

public class HBaseJunitRuleTest {

    @ClassRule
    public static HBaseJunitRule hBaseJunitRule = new HBaseJunitRule();

    @Test
    public void clusterId_notNull() throws IOException {
        Configuration conf = hBaseJunitRule.getHBaseConfiguration();
        try(Connection conn = ConnectionFactory.createConnection(conf)) {
            assertThat(conn.getAdmin().getClusterStatus().getClusterId(), is(notNullValue()));
        }
    }

    @Test
    public void createTable_successfully() throws IOException {
        Configuration conf = hBaseJunitRule.getHBaseConfiguration();
        try(Connection conn = ConnectionFactory.createConnection(conf)) {
            TableName table = TableName.valueOf("test_table");
            HTableDescriptor descriptor = new HTableDescriptor(table);
            descriptor.addFamily(new HColumnDescriptor("d"));

            try(Admin admin = conn.getAdmin()) {
                admin.createTable(descriptor);
            }

            try(Table t = conn.getTable(table)){
                Put p = new Put(Bytes.toBytes("hello"));
                p.addColumn(Bytes.toBytes("d"), Bytes.toBytes("col"), Bytes.toBytes("value"));
                t.put(p);
            }

            assertThat(conn.getAdmin().tableExists(table), is(true));

            try(Table t = conn.getTable(table)){
                Get g = new Get(Bytes.toBytes("hello"));
                Result r = t.get(g);

                assertThat(r, is(notNullValue()));
                assertThat(r.isEmpty(), is(false));
                assertThat(r.containsColumn(Bytes.toBytes("d"), Bytes.toBytes("col")), is(true));
            }
        }
    }
}
