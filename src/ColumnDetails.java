import java.util.Comparator;

public class ColumnDetails {
    public String column_name;
    public String data_type;
    public int ordinal_position;
    public boolean is_nullable;
    public Object value;

    public ColumnDetails(String column_name, String data_type, int ordinal_position, boolean is_nullable){
        this.column_name = column_name;
        this.data_type = data_type;
        this.ordinal_position = ordinal_position;
        this.is_nullable = is_nullable;
    }

    static class ColumnComparator implements Comparator<ColumnDetails> {

        @Override
        public int compare(ColumnDetails col1, ColumnDetails col2) {
            return ((Integer)col1.ordinal_position).compareTo((Integer)col2.ordinal_position);
        }
    }
}
