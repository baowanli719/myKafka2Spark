package com.gszq.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HbaseConn {

    private static Configuration krbConf = getKrbHbaseConf();
    private static Properties hbaseproperties = MyPropertiesUtil.load();
    private static Admin admin =null;
    private static Connection connection =null;

    private static String zookeeperQuorum =  hbaseproperties.getProperty("hbase.zookeeper.quorum");
    private static String clientPort = "2181";
    private static String principal = hbaseproperties.getProperty("hbase.master.principal");


    private static String keytabPath = hbaseproperties.getProperty("hbase.master.keytab.file");

    public static Configuration getKrbHbaseConf(){


        final Configuration conf = HBaseConfiguration.create();
        File corefile=new File(hbaseproperties.getProperty("core-site.path")); //根据路径创建File类对象--这里路径即使错误也不会报错，因为只是产生File对象，还并未与计算机文件读写有关联
        System.out.println(hbaseproperties.getProperty("core-site.path"));
        File hbasefile=new File(hbaseproperties.getProperty("hdfs-site.path")); //根据路径创建File类对象--这里路径即使错误也不会报错，因为只是产生File对象，还并未与计算机文件读写有关联
        File hdfsfile=new File(hbaseproperties.getProperty("hbase-site.path")); //根据路径创建File类对象--这里路径即使错误也不会报错，因为只是产生File对象，还并未与计算机文件读写有关联
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hbase.security.authentication", "kerberos");

        try
        {
            FileInputStream coreStream=new FileInputStream(corefile);//与根据File类对象的所代表的实际文件建立链接创建fileInputStream对象
            conf.addResource(coreStream);
        }
        catch (FileNotFoundException e)
        {

            System.out.println("文件不存在或者文件不可读或者文件是目录");
        }
        try
        {
            FileInputStream hbaseStream=new FileInputStream(hbasefile);//与根据File类对象的所代表的实际文件建立链接创建fileInputStream对象
            conf.addResource(hbaseStream);
        }
        catch (FileNotFoundException e)
        {

            System.out.println("文件不存在或者文件不可读或者文件是目录");
        }

        try
        {
            FileInputStream hdfsStream=new FileInputStream(hdfsfile);//与根据File类对象的所代表的实际文件建立链接创建fileInputStream对象
            conf.addResource(hdfsStream);
        }
        catch (FileNotFoundException e)
        {

            System.out.println("文件不存在或者文件不可读或者文件是目录");
        }
        conf.set("hbase.zookeeper.quorum", zookeeperQuorum);
        conf.set("hbase.zookeeper.property.clientPort", clientPort);
        conf.setInt("hbase.rpc.timeout", 20000);
        conf.setInt("hbase.client.operation.timeout", 30000);
        conf.setInt("hbase.client.scanner.timeout.period", 200000);

        try {
            // 使用票据登陆Kerberos
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
            ExecutorService executor = Executors.newFixedThreadPool(10);
            connection = ConnectionFactory.createConnection(conf, executor);
            //connection = ConnectionFactory.createConnection(conf);
            System.out.println(connection.hashCode());
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        return conf;

    }
    public  synchronized Connection createConnection(){
        try {
            // 使用票据登陆Kerberos
            Configuration conf = getKrbHbaseConf();
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
            ExecutorService executor = Executors.newFixedThreadPool(10);
            connection = ConnectionFactory.createConnection(conf, executor);
            System.out.println(connection.hashCode());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }

}
