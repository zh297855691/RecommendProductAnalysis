import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by lenovo on 2017/2/15.
 */
public class HBasetest {

    private static Configuration conf;
    private static Connection conn;
    private static Table tableMerge;
    private String tableName;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum","192.168.119.251:2181,192.168.119.252:2181,192.168.119.253:2181");
//        conf.set("hbase.zookeeper.quorum", Utils.loadProperties("contact-realTimeEngine.properties").getProperty("zookeeper_servers_ip"));
    }

    //��ѯ�����Ƿ��Ѵ��ڼ�¼�������ڷ��ؽ��ֵ
    public String queryHBase(String tableName,String rowkey) {

        Boolean flag = false;
        Get getkey = new Get(rowkey.getBytes());
        String value = null;
        try {
            Result result = getTable(tableName).get(getkey);
            System.out.println(Bytes.toString(result.getValue("record".getBytes(),"age".getBytes())));
            value = Bytes.toString(result.getValue("record".getBytes(),"age".getBytes()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return value;
    }

    public synchronized Boolean insertHbase(String tableName,String rowkey, String value){
        Boolean flag=false;
        try {
            Put put = new Put(rowkey.getBytes());
            put.addColumn("record".getBytes(),"value".getBytes(),value.getBytes());
            getTable(tableName).put(put);
            flag = true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return flag;
    }

    public static Table getTable(String tableName) {

        if(tableMerge==null){
            try {
                tableMerge=getConnection().getTable(TableName.valueOf(tableName));
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return tableMerge;
    }

    public static Connection getConnection(){
        if(conn == null){
            try {
                conn = ConnectionFactory.createConnection(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return conn;
    }

    //���ݹ���ID������ӦԤ���������Ʊ�(����Phoenix��)
    public static void createTableByRuleID(Integer ruleID) {

        String tableName = "warn_upgradeControl_" + ruleID;
        TableName ruleTable = TableName.valueOf(tableName);
        HTableDescriptor tableDescriptor = new HTableDescriptor(ruleTable);
        tableDescriptor.addFamily(new HColumnDescriptor("details".getBytes()));
        try {

            Admin admin = getConnection().getAdmin();
            if (admin.tableExists(ruleTable)) {
                System.out.println("�����������Ʊ��Ѿ����ڣ�����Ϊ�� " + tableName);
            } else {
                admin.createTable(tableDescriptor);
                System.out.println("�����������Ʊ��Ѿ������ɹ�������Ϊ��" + tableName);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void getScanner() {

        Scan scan = new Scan();
        try {
           Table t = getConnection().getTable(TableName.valueOf("rctl2.TRADE_INFO_DETAIL"));
//           scan.addColumn("TRADEINFO".getBytes(),"CUTOFFDAY".getBytes());
//           scan.addColumn("TRADEINFO".getBytes(),"TRANSIDO".getBytes());
           ResultScanner scanner = t.getScanner(scan);
           for (Result r : scanner) {
//               Result r = scanner.next();
               for(Cell c : r.rawCells()) {
                   String k = Bytes.toString(CellUtil.cloneRow(c));
                   String family = Bytes.toString(CellUtil.cloneFamily(c));
                   String q = Bytes.toString(CellUtil.cloneQualifier(c));
                   String v = Bytes.toString(CellUtil.cloneValue(c));
                   System.out.println("ROWKEYΪ��"+k+"Family:" + family + " ������" + q +" ֵΪ��" + v);
               }

            }
        }catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    public static void main(String[] args) {
        HBasetest bu = new HBasetest();
        bu.getScanner();
//        System.out.println(bu.queryHBase("rctl2.TRADE_INFO_DETAIL","001"));
    }

}