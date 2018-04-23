public class UnknownDataTypeException extends Exception {
    public UnknownDataTypeException(String s) {
        super(s + " datatype is not defined.");
    }
}
