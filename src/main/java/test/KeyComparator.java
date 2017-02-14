package test;



public class KeyComparator {
    /** Size of the key type field in bytes. */
    public static final int TYPE_SIZE = Bytes.SIZEOF_BYTE;

    /** Size of the row length field in bytes. */
    public static final int ROW_LENGTH_SIZE = Bytes.SIZEOF_SHORT;

    /** Size of the family length field in bytes. */
    public static final int FAMILY_LENGTH_SIZE = Bytes.SIZEOF_BYTE;

    /** Size of the timestamp field in bytes. */
    public static final int TIMESTAMP_SIZE = Bytes.SIZEOF_LONG;

    // Size of the timestamp and type byte on end of a key -- a long + a byte.
    public static final int TIMESTAMP_TYPE_SIZE = TIMESTAMP_SIZE + TYPE_SIZE;

    // Size of the length shorts and bytes in key.
    public static final int KEY_INFRASTRUCTURE_SIZE = ROW_LENGTH_SIZE
            + FAMILY_LENGTH_SIZE + TIMESTAMP_TYPE_SIZE;

    // How far into the key the row starts at. First thing to read is the short
    // that says how long the row is.
    public static final int ROW_OFFSET =
            Bytes.SIZEOF_INT /*keylength*/ +
                    Bytes.SIZEOF_INT /*valuelength*/;

    // Size of the length ints in a KeyValue datastructure.
    public static final int KEYVALUE_INFRASTRUCTURE_SIZE = ROW_OFFSET;

    volatile boolean ignoreTimestamp = false;
    volatile boolean ignoreType = false;

    public int compare(byte[] left, int loffset, int llength, byte[] right,
                       int roffset, int rlength) {
        // Compare row
        short lrowlength = Bytes.toShort(left, loffset);
        short rrowlength = Bytes.toShort(right, roffset);
        int compare = compareRows(left, loffset + Bytes.SIZEOF_SHORT,
                lrowlength, right, roffset + Bytes.SIZEOF_SHORT, rrowlength);
        if (compare != 0) {
            return compare;
        }

        // Compare the rest of the two KVs without making any assumptions about
        // the common prefix. This function will not compare rows anyway, so we
        // don't need to tell it that the common prefix includes the row.
        return compareWithoutRow(0, left, loffset, llength, right, roffset,
                rlength, rrowlength);
    }

    /**
     * Compare columnFamily, qualifier, timestamp, and key type (everything
     * except the row). This method is used both in the normal comparator and
     * the "same-prefix" comparator. Note that we are assuming that row portions
     * of both KVs have already been parsed and found identical, and we don't
     * validate that assumption here.
     * @param commonPrefix
     *          the length of the common prefix of the two key-values being
     *          compared, including row length and row
     */
    private int compareWithoutRow(int commonPrefix, byte[] left, int loffset,
                                  int llength, byte[] right, int roffset, int rlength, short rowlength) {
        /***
         * KeyValue Format and commonLength:
         * |_keyLen_|_valLen_|_rowLen_|_rowKey_|_famiLen_|_fami_|_Quali_|....
         * ------------------|-------commonLength--------|--------------
         */
        int commonLength = ROW_LENGTH_SIZE + FAMILY_LENGTH_SIZE + rowlength;

        // commonLength + TIMESTAMP_TYPE_SIZE
        int commonLengthWithTSAndType = TIMESTAMP_TYPE_SIZE + commonLength;
        // ColumnFamily + Qualifier length.
        int lcolumnlength = llength - commonLengthWithTSAndType;
        int rcolumnlength = rlength - commonLengthWithTSAndType;

        byte ltype = left[loffset + (llength - 1)];
        byte rtype = right[roffset + (rlength - 1)];

        // If the column is not specified, the "minimum" key type appears the
        // latest in the sorted order, regardless of the timestamp. This is used
        // for specifying the last key/value in a given row, because there is no
        // "lexicographically last column" (it would be infinitely long). The
        // "maximum" key type does not need this behavior.
        if (lcolumnlength == 0 && ltype == Type.Minimum.getCode()) {
            // left is "bigger", i.e. it appears later in the sorted order
            return 1;
        }
        if (rcolumnlength == 0 && rtype == Type.Minimum.getCode()) {
            return -1;
        }

        int lfamilyoffset = commonLength + loffset;
        int rfamilyoffset = commonLength + roffset;

        // Column family length.
        int lfamilylength = left[lfamilyoffset - 1];
        int rfamilylength = right[rfamilyoffset - 1];
        // If left family size is not equal to right family size, we need not
        // compare the qualifiers.
        boolean sameFamilySize = (lfamilylength == rfamilylength);
        int common = 0;
        if (commonPrefix > 0) {
            common = Math.max(0, commonPrefix - commonLength);
            if (!sameFamilySize) {
                // Common should not be larger than Math.min(lfamilylength,
                // rfamilylength).
                common = Math.min(common, Math.min(lfamilylength, rfamilylength));
            } else {
                common = Math.min(common, Math.min(lcolumnlength, rcolumnlength));
            }
        }
        if (!sameFamilySize) {
            // comparing column family is enough.
            return Bytes.compareTo(left, lfamilyoffset + common, lfamilylength
                    - common, right, rfamilyoffset + common, rfamilylength - common);
        }
        // Compare family & qualifier together.
        final int comparison = Bytes.compareTo(left, lfamilyoffset + common,
                lcolumnlength - common, right, rfamilyoffset + common,
                rcolumnlength - common);
        if (comparison != 0) {
            return comparison;
        }
        return compareTimestampAndType(left, loffset, llength, right, roffset,
                rlength, ltype, rtype);
    }

    private int compareTimestampAndType(byte[] left, int loffset, int llength,
                                        byte[] right, int roffset, int rlength, byte ltype, byte rtype) {
        int compare;
        if (!this.ignoreTimestamp) {
            // Get timestamps.
            long ltimestamp = Bytes.toLong(left,
                    loffset + (llength - TIMESTAMP_TYPE_SIZE));
            long rtimestamp = Bytes.toLong(right,
                    roffset + (rlength - TIMESTAMP_TYPE_SIZE));
            compare = compareTimestamps(ltimestamp, rtimestamp);
            if (compare != 0) {
                return compare;
            }
        }

        if (!this.ignoreType) {
            // Compare types. Let the delete types sort ahead of puts; i.e. types
            // of higher numbers sort before those of lesser numbers. Maximum (255)
            // appears ahead of everything, and minimum (0) appears after
            // everything.
            return (0xff & rtype) - (0xff & ltype);
        }
        return 0;
    }

    public int compare(byte[] left, byte[] right) {
        return compare(left, 0, left.length, right, 0, right.length);
    }

    public int compareRows(byte [] left, int loffset, int llength,
                           byte [] right, int roffset, int rlength) {
        return Bytes.compareTo(left, loffset, llength, right, roffset, rlength);
    }

    int compareTimestamps(final long ltimestamp, final long rtimestamp) {
        // The below older timestamps sorting ahead of newer timestamps looks
        // wrong but it is intentional. This way, newer timestamps are first
        // found when we iterate over a memstore and newer versions are the
        // first we trip over when reading from a store file.
        if (ltimestamp < rtimestamp) {
            return 1;
        } else if (ltimestamp > rtimestamp) {
            return -1;
        }
        return 0;
    }
}