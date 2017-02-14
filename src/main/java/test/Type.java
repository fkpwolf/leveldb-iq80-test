package test;



public enum Type {
    Minimum((byte) 0),
    Put((byte) 4),

    Delete((byte) 8),
    DeleteColumn((byte) 12),
    UndeleteColumn((byte) 13),
    DeleteFamily((byte) 14),

    // Maximum is used when searching; you look from maximum on down.
    Maximum((byte) 255);

    private final byte code;

    Type(final byte c) {
        this.code = c;
    }

    public byte getCode() {
        return this.code;
    }

    /**
     * Cannot rely on enum ordinals . They change if item is removed or moved.
     * Do our own codes.
     * @return Type associated with passed code.
     */
    public static Type codeToType(final byte b) {
        for (Type t : Type.values()) {
            if (t.getCode() == b) {
                return t;
            }
        }
        throw new RuntimeException("Unknown code " + b);
    }
}