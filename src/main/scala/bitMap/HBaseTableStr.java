package bitMap;/**
 * Created by spark01 on 18-7-5.
 */

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import static java.awt.Container.log;


/**
 * @author: spark01
 * @date: 18-7-5
 * @description:
 */
public class HBaseTableStr {



    private byte[][] getSplitKeys() {
        //==========================================================0
        //创建一个Stirng类型的数组，存储每一个分区的rowKey的边界
        String[] keys = new String[] { "10|", "20|", "30|", "40|", "50|",
                "60|", "70|", "80|", "90|" };

        //==========================================================1
        /*
        *gcs:
        *创建一个二维数组，用于存储rowKey的边界值
        */
        byte[][] splitKeys = new byte[keys.length][];

        //==========================================================2
        /*
        *gcs:
        *这个Bytes 的类型是HBase的类型的
        * Bytes是一种可以将Byte类型的数据转换为其他类型数据的类型。
        * 设定Byte的类型为向上排序
        * 指定排序的类型为升序排序
        */
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);//升序排序

        //==========================================================3
        /*
        *gcs:
        *将准备分区的10,20,30放到刚刚创建成功的TreeSet当中
        */
        for (int i = 0; i < keys.length; i++) {
            rows.add(Bytes.toBytes(keys[i]));
        }


        //==========================================================4
        /*
        *gcs:
        *将rows当中的元素变成一个升序的遍历器
        * 所以说，升序的操作是在这里实现的
        */
        Iterator<byte[]> rowKeyIter = rows.iterator();
        int i=0;
        while (rowKeyIter.hasNext()) {
            byte[] tempRow = rowKeyIter.next();
            rowKeyIter.remove(); //gcs:移除迭代器当中的next之前的那个元素
            splitKeys[i] = tempRow; //gcs:将tempRow存放到splitKeys数组当中
            i++;
        }
        return splitKeys; //gcs:最后将这个splitKeys的数组返回去
    }


    /**
    * <b>author:</b> gcs <br>
    * <b>data:</b> 18-7-5 <br>
    * <b>description:</b><br>
     *     这是创建表的函数 <br>
    * <b>param:</b><br>
    * <b>return:</b><br>
    */
    public boolean createTableBySplitKeys(String tableName, List<String> columnFamily) {
        try {

            //==========================================================5
            /*
            *gcs:
            *检测传进来的tableName是不是空字符串。以及传进来的List数组是不是空
            */
            if (StringUtils.isBlank(tableName) || columnFamily == null
                    || columnFamily.size() < 0) {
//                log.error("===Parameters tableName|columnFamily should not be null,Please check!===");
            }

            //==========================================================6
            /*
            *gcs:
            *这里还缺少一个配置项啊。这个conf的配置项是指定Hbase的zookeeper的地址和端口的地址啊
            */
            HBaseAdmin admin = new HBaseAdmin(conf);

            //==========================================================7
            /*
            *gcs:
            *查看要创建的tableName是否已经存在了
            */
            if (admin.tableExists(tableName)) {
                return true;
            } else {
                //==========================================================8
                /*
                *gcs:
                *如果要使用的表此时没有存在，就会创建表
                * HTableDescriptor 这个类是用来描述HBase表的属性的。这个表的表名，列族之类的信息
                */
                HTableDescriptor tableDescriptor = new HTableDescriptor(
                        TableName.valueOf(tableName));

                //==========================================================9
                /*
                *gcs:
                *columnFamily 是一个列族的集合。把每一个列族都放到这个HBase的表的描述当中
                */
                for (String cf : columnFamily) {
                    tableDescriptor.addFamily(new HColumnDescriptor(cf));
                }

                //==========================================================10
                /*
                *gcs:
                *调用上面的 getSplitKeys 函数，获得splitKeys的数组
                */
                byte[][] splitKeys = getSplitKeys();

                //==========================================================11
                /*
                *gcs:
                *用指定的分区的方式，创建HBase表
                * splitKeys当中就存放着我们的分区的数组
                */
                admin.createTable(tableDescriptor,splitKeys);//指定splitkeys
//                log.info("===Create Table " + tableName
//                        + " Success!columnFamily:" + columnFamily.toString()
//                        + "===");
            }
        } catch (MasterNotRunningException e) {
            // TODO Auto-generated catch block
//            log.error(e);
            return false;
        } catch (ZooKeeperConnectionException e) {
            // TODO Auto-generated catch block
//            log.error(e);
            return false;
        } catch (IOException e) {
            // TODO Auto-generated catch block
//            log.error(e);
            return false;
        }
        return true;
    }



    public class TestHBasePartition {
        public static void main(String[] args) throws Exception{
            //==========================================================17
            /*
            *gcs:
            *打开一个表。conf配置项中指定这个表的zookeeper地址和端口号
            */
            HBaseAdmin admin = new HBaseAdmin(conf);
            //==========================================================18
            /*
            *gcs:
            *连接zookeeper地址中的表名为 testbase的表
            */
            HTable table = new HTable(conf, "testhbase");

            //==========================================================19
            /*
            *gcs:
            *将数据批量的插入到这个表中
            */
            table.put(batchPut());
        }


        /**
        * <b>author:</b> gcs <br>
        * <b>data:</b> 18-7-5 <br>
        * <b>description:</b><br>
         *     获得一个随机数
        * <b>param:</b><br>
        * <b>return:</b><br>
        */
        private static String getRandomNumber(){
            String ranStr = Math.random()+"";
            int pointIndex = ranStr.indexOf(".");
            return ranStr.substring(pointIndex+1, pointIndex+3);
        }


        //==========================================================12
        /*
        *gcs:
        *批量往表中插入数据
        */
        /**
        * <b>author:</b> gcs <br>
        * <b>data:</b> 18-7-5 <br>
        * <b>description:</b><br>
         *     批量的往HBase当中插入元素
        * <b>param:</b><br>
        * <b>return:</b><br>
        */
        private static List<Put> batchPut(){
            List<Put> list = new ArrayList<Put>(); //gcs:所谓的批量插入，就是将要插入的元素先放到一个数组当中。之后再进行批量的插入元素

            //==========================================================13
            /*
            *gcs:
            *批量的往数组中插入10000条数据
            */
            for(int i=1;i<=10000;i++){

                //==========================================================14
                /*
                *gcs:
                *指定rowKey
                */
                byte[] rowkey = Bytes.toBytes(getRandomNumber()+"-"+System.currentTimeMillis()+"-"+i);

                //==========================================================15
                /*
                *gcs:
                *创建一个put对象。将rowKey放到里面
                */
                Put put = new Put(rowkey);

                //==========================================================15
                /*
                *gcs:
                *把列族名为info，列为name的表中插入一条数据，这个数据的value值为 zs+i
                */
                put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("zs"+i));
                //==========================================================16
                /*
                *gcs:
                *将这个put数组放入到list当中
                */
                list.add(put);
            }
            return list;
        }
    }


}
