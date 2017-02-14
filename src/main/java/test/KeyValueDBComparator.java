package test;


import org.iq80.leveldb.DBComparator;

public  class KeyValueDBComparator implements DBComparator {

    public int compare(byte[] left, byte[] right) {
        return new KeyComparator().compare(left, right);
    }

    public byte[] findShortSuccessor(byte[] key) {
        return key;
    }

    public byte[] findShortestSeparator(byte[] start, byte[] limit) {
        return start;
    }

    public String name() {
        return "hbase-kv";
    }
}

