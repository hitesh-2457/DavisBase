public enum NodeTypes {
    InternalNode(0x05), LeafNode(0x0D);

    private int Value;

    NodeTypes(int Value) {
        this.Value = Value;
    }

    public int getValue() {
        return Value;
    }
}
