/**
 * Created by fan on 2016/12/9.
 *
 * https://github.com/dain/leveldb
 *
 *
 */
package test;

import org.apache.hadoop.hbase.KeyValue;
import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import java.io.*;
import java.util.Date;


public class Main {
    public static void main(String[] args) throws Exception{
         int blockSize = 1024; // cdap-default.xml
         long cacheSize = 104857600;

        Options options = new Options();
        options.createIfMissing(false);
        options.errorIfExists(false);
        options.comparator(new KeyValueDBComparator());
        options.blockSize(blockSize);
        options.cacheSize(cacheSize);

        DB db = factory.open(new File("cdap_default.anotherTable"), //all *.meta will throw exception
                options);

        DBIterator iterator = db.iterator();
        int i =0;
        try {
            for(iterator.seekToFirst(); iterator.hasNext() && i < 10; iterator.next()) {
                i++;
               // Object key = (iterator.peekNext().getKey());
                //Object value = (iterator.peekNext().getValue());

                String key = asString(iterator.peekNext().getKey());
                String value = asString(iterator.peekNext().getValue());

                System.out.println(key+" = "+value);
                /*
                X��nip�	���x@ = 69.181.160.120
                X��nuri�	���x@ = /REST/API/LATEST/SERVER?_=1423341313766|
                X��sbrowser�	���x@ = Chrome
                X��sdevice�	���x@ = Personal computer
                X��shttpStatus�	���x@ = �
                这个 key 需要解析下，里面包含了 rowkey+column-name
                * */
                //key,value如何表现一条记录？hbase-kv?
                KeyValue kv = KeyValue.createKeyValueFromKey(iterator.peekNext().getKey());
                System.out.println(kv);
                System.out.println(new Date(kv.getTimestamp()));
            }
        } finally {
            // Make sure you close the iterator to avoid resource leaks.
            iterator.close();
        }
    }
}

