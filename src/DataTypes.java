public enum DataTypes {
	TINYINT(0x04, 1), SMALLINT(0x05, 2), INT(0x06, 4), BIGINT(0X07, 8), REAL(0X08, 4), DOUBLE(0x09, 8), DATETIME(0x0A,
			8), DATE(0x0B, 8), TEXT(0x0C, -1);

	private int header;
	private int size;

	DataTypes(int header, int size) {
		this.header = header;
		this.size = size;
	}

	public int getHeader() {
		return header;
	}

	public int getSize() {
		return size;
	}
}
