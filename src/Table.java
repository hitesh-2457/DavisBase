import java.io.IOException;
import java.io.RandomAccessFile;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.*;
import java.time.temporal.TemporalAccessor;
import java.util.*;

public class Table {
	private String TableName = "";
	private String Path = "";
	private boolean IsMetaTable = false;
	private static final int FILESIZE = 512;
	private int NoPages = 0;
	private RandomAccessFile binaryFile;
	private Map<Integer, RecordCell> Columns;

	public void initialize(String path, String tableName) throws Exception {
		this.TableName = tableName;
		this.Path = path;
		binaryFile = new RandomAccessFile(this.Path + "/" + this.TableName + ".tbl", "rw");
		IsMetaTable = this.Path.contains("catalog");
		NoPages = (int) binaryFile.length() / FILESIZE;
		if (!IsMetaTable)
			fetchMetaData();
	}

	private void fetchMetaData() throws Exception {
		// fetch root_page.
		if (!IsMetaTable) {
			Table metaTable = new Table();
			metaTable.initialize("data/catalog", "davisbase_columns");
			Columns = metaTable.selectRecords(new String[] { "*" }, // Columns needed
					new String[] { "table_name", "=", TableName }); // where tableName = TableName condition
		}
	}

	public boolean isMeta() {
		return IsMetaTable;
	}

	public void createFile(String path, String tableName) throws IOException {
		createTableFile(path, tableName);
		setLeafHeaders(1);
	}

	private void createTableFile(String path, String tableName) throws IOException {
		binaryFile = new RandomAccessFile(path + "/" + tableName + ".tbl", "rw");
		binaryFile.setLength(0);
		binaryFile.setLength(FILESIZE);
		this.NoPages = 1;
	}

	private void setLeafHeaders(int page) throws IOException {
		int fileBegin = (page - 1) * FILESIZE;
		binaryFile.seek(fileBegin + 0);
		binaryFile.writeByte(NodeTypes.LeafNode.getValue());

		binaryFile.seek(fileBegin + 2);
		binaryFile.writeShort(512);

		binaryFile.seek(fileBegin + 4);
		binaryFile.writeInt(0xFFFFFFFF);
	}

	private void setNodeHeaders(int page) throws IOException {
		int fileBegin = (page - 1) * FILESIZE;
		binaryFile.seek(fileBegin + 0);
		binaryFile.writeByte(NodeTypes.InternalNode.getValue());

		binaryFile.seek(fileBegin + 2);
		binaryFile.writeShort(512);

		binaryFile.seek(fileBegin + 4);
		binaryFile.writeInt(0xFFFFFFFF);
	}

	public boolean isLeaf(int page) throws IOException {
		binaryFile.seek(((page - 1) * FILESIZE) + 0);
		return (int) binaryFile.readByte() == NodeTypes.LeafNode.getValue();
	}

	public boolean isNode(int page) throws IOException {
		binaryFile.seek(((page - 1) * FILESIZE) + 0);
		return (int) binaryFile.readByte() == NodeTypes.InternalNode.getValue();
	}

	public Map<Integer, String> getColumnNames() {
		Map<Integer, String> colNames = new LinkedHashMap<>();
		if (IsMetaTable && this.TableName.equalsIgnoreCase("davisbase_columns")) {
			colNames.put(1, "rowid");
			colNames.put(2, "table_name");
			colNames.put(3, "column_name");
			colNames.put(4, "data_type");
			colNames.put(5, "ordinal_position");
			colNames.put(6, "is_nullable");
		} else if (IsMetaTable && this.TableName.equalsIgnoreCase("davisbase_tables")) {
			colNames.put(1, "rowid");
			colNames.put(2, "table_name");
		} else {
			Object[] cols = Columns.values().toArray();
			for (int i = 0; i < cols.length; i++) {
				String[] columnData = ((RecordCell) cols[i]).getPayload().getData();
				colNames.put(Integer.parseInt(columnData[3]), columnData[1]);
			}
		}
		return colNames;
	}

	public Map<Integer, String> getColumnDataTypes() {
		Map<Integer, String> colDataTypes = new LinkedHashMap<>();
		Object[] cols = Columns.values().toArray();
		for (int i = 0; i < cols.length; i++) {
			String[] columnData = ((RecordCell) cols[i]).getPayload().getData();
			colDataTypes.put(Integer.parseInt(columnData[3]), columnData[2]);
		}
		return colDataTypes;
	}

	public Map<Integer, String> getColumnNullable() {
		Map<Integer, String> isNullable = new LinkedHashMap<>();
		Object[] cols = Columns.values().toArray();
		for (int i = 0; i < cols.length; i++) {
			String[] columnData = ((RecordCell) cols[i]).getPayload().getData();
			isNullable.put(Integer.parseInt(columnData[3]), columnData[4]);
		}
		return isNullable;
	}

	public Map<Integer, ColumnDetails> getColumnDetails() {
		Map<Integer, ColumnDetails> colDetails = new LinkedHashMap<>();
		Object[] cols = Columns.values().toArray();
		for (int i = 0; i < cols.length; i++) {
			String[] columnData = ((RecordCell) cols[i]).getPayload().getData();
			colDetails.put(Integer.parseInt(columnData[3]), new ColumnDetails(columnData[1], columnData[2],
					Integer.parseInt(columnData[3]), columnData[4].equalsIgnoreCase("yes")));
		}
		return colDetails;
	}

	public int fetchNoRecords() throws IOException {
		int page = 1;
		int total = 0;
		while (page != 0xFFFFFFFF) {
			total += findNoRecordsInPage(page);
			page = fetchNextLeafPage(page);
		}
		return total;
	}

	private int findNoRecordsInPage(int page) throws IOException {
		binaryFile.seek(((page - 1) * FILESIZE) + 1);
		return (int) binaryFile.readByte();
	}

	public int fetchNextLeafPage(int page) throws IOException {
		binaryFile.seek(((page - 1) * FILESIZE) + 4);
		return binaryFile.readInt();
	}

	public boolean canInsert(int page, int size) throws IOException {
		// size + 2, considering the pointer to record in header
		return (size + 2) < (FILESIZE - headerSize(page) - dataSize(page));
	}

	private int dataSize(int page) throws IOException {
		int fstRecLoc = fetchTopRecLoc(page);
		return FILESIZE - fstRecLoc;
	}

	private int fetchTopRecLoc(int page) throws IOException {
		binaryFile.seek(((page - 1) * FILESIZE) + 2);
		return (int) binaryFile.readShort();
	}

	public int headerSize(int page) throws IOException {
		// fixed header size is 8 (1 + 1 + 2 + 4) for leaf or an internal node
		int noRec = findNoRecordsInPage(page);
		return 8 + (2 * noRec);
	}

	private void insertRec(int page, int payLoadSize, RecordCell dataCell) throws IOException {
		this.insertRec(page, payLoadSize, dataCell, -1);
	}

	private void insertRec(int page, int payLoadSize, RecordCell dataCell, int location) throws IOException {
		// TopMost Record - data length
		int newFstRecLoc = fetchTopRecLoc(page) - payLoadSize;

		// write data
		binaryFile.seek((page - 1) * FILESIZE + newFstRecLoc);
		binaryFile.writeShort(dataCell.getPayLoadSize());
		binaryFile.writeInt(dataCell.getRowId());

		PayLoad payload = dataCell.getPayload();
		binaryFile.writeByte(payload.getNoColumns());

		byte[] dataTypes = payload.getDataTypes();
		binaryFile.write(dataTypes);

		String data[] = payload.getData();

		for (int i = 0; i < dataTypes.length; i++) {
			switch (dataTypes[i]) {
			case 0x00:
				binaryFile.writeByte(0);
				break;
			case 0x01:
				binaryFile.writeShort(0);
				break;
			case 0x02:
				binaryFile.writeInt(0);
				break;
			case 0x03:
				binaryFile.writeLong(0);
				break;
			case 0x04:
				binaryFile.writeByte(new Byte(data[i + 1]));
				break;
			case 0x05:
				binaryFile.writeShort(new Short(data[i + 1]));
				break;
			case 0x06:
				binaryFile.writeInt(new Integer(data[i + 1]));
				break;
			case 0x07:
				binaryFile.writeLong(new Long(data[i + 1]));
				break;
			case 0x08:
				binaryFile.writeFloat(new Float(data[i + 1]));
				break;
			case 0x09:
				binaryFile.writeDouble(new Double(data[i + 1]));
				break;
			case 0x0A:
				long datetime = binaryFile.readLong();
				ZoneId zoneId = ZoneId.of("America/Chicago");
				Instant x = Instant.ofEpochSecond(datetime);
				ZonedDateTime zdt2 = ZonedDateTime.ofInstant(x, zoneId);
				zdt2.toLocalTime();
				break;
			case 0x0B:
				long date = binaryFile.readLong();
				ZoneId zoneId1 = ZoneId.of("America/Chicago");
				Instant x1 = Instant.ofEpochSecond(date);
				ZonedDateTime zdt3 = ZonedDateTime.ofInstant(x1, zoneId1);
				break;
			default:
				binaryFile.writeBytes(data[i + 1]);
				break;
			}
		}

		// write the new TopMost Record to header
		binaryFile.seek((page - 1) * FILESIZE + 2);
		binaryFile.writeShort(newFstRecLoc);

		if (location == -1) {
			// increment the count on file
			int count = findNoRecordsInPage(page);
			binaryFile.seek((page - 1) * FILESIZE + 1);
			binaryFile.writeByte(count + 1);

			// add the pointer to the new rec to the pointer list.
			binaryFile.seek((page - 1) * FILESIZE + 8 + ((count == 0) ? 0 : (count * 2)));
			binaryFile.writeShort(newFstRecLoc);
		} else {
			int count = findNoRecordsInPage(page);
			binaryFile.seek((page-1)*FILESIZE+8);
			int i = 0;
			int ptr = binaryFile.readShort();
			while(i<count&&ptr!=location) {
				ptr = binaryFile.readShort();
				i++;
			}
			if(ptr == location) {
				binaryFile.seek((page-1)*FILESIZE+8+(i*2));
				binaryFile.writeShort(newFstRecLoc);
			}
		}
	}

	public void insertToLeaf(String[] colNames, String[] values) throws Exception {
		// +1 byte for number of columns, +6 bytes for the record headers, every column
		// other than rowid has a byte header
		Map<Integer, String> dataTypes = getColumnDataTypes();
		Map<Integer, ColumnDetails> columnDetails = getColumnDetails();
		Map<Integer, String> isNullable = getColumnNullable();
		Object[] nullables = isNullable.values().toArray();
		byte[] dataHeaders = new byte[columnDetails.size() - 1];
		int headerPointer = 0;

		for (int i = 0; i < values.length; i++) {
			if (values[i].equalsIgnoreCase("null") && ((String) nullables[i]).equals("NO")) {
				System.out.println("Cannot insert NULL values in NOT NULL field");
				return;
			}
		}

		int dataSize = (columnDetails.size() - 1) + 1 + 6;

		for (int i : columnDetails.keySet()) {
			ColumnDetails col = columnDetails.get(i);
			if (col.column_name.equalsIgnoreCase("rowid")) {
				continue;
			}
			int indx = findIndx(colNames, col.column_name);
			if (indx != -1) {
				dataSize += getDataTypeSize(col.data_type, values[indx].length());
				dataHeaders[headerPointer++] = (byte) getDataTypeHeader(col.data_type, false, values[indx].length());
			} else if (indx == -1 && col.is_nullable)
				dataHeaders[headerPointer++] = (byte) getDataTypeHeader(col.data_type, true, 0);
			else
				throw new Exception("Could not find column '" + col.column_name + "'");
		}

		int pageNo = fetchLastPage();
		// check leaf size
		byte[] plDataType = new byte[columnDetails.size() - 1];
		String[] dataTypeStr = new String[columnDetails.size()];
		dataTypes.values().toArray(dataTypeStr);
		int payLoadSize = getPayloadSize(values, plDataType, dataTypeStr);
		payLoadSize = payLoadSize + 6;

		boolean canInsert = canInsert(pageNo, payLoadSize);

		if (canInsert) {
			RecordCell cell = createCell(pageNo, fetchNextRowID(), (short) payLoadSize, plDataType, values);
			insertRec(pageNo, payLoadSize, cell);
		} else {
			int rowid = fetchNextRowID();
			int pNo = splitLeafPage(pageNo);
			RecordCell cell = createCell(pNo, rowid, (short) payLoadSize, plDataType, values);
			insertRec(pNo, payLoadSize, cell);
		}
	}

	public void insertToMeta(String[] colNames, String[] values) throws Exception {
		int pageNo = fetchLastPage();
		int noColmns;
		Map<Integer, String> dataTypes = new LinkedHashMap<>();
		// check leaf size
		if (IsMetaTable && this.TableName.equalsIgnoreCase("davisbase_tables")) {
			noColmns = 2;
			dataTypes.put(1, "int");
			dataTypes.put(2, "text");
		} else if (IsMetaTable && this.TableName.equalsIgnoreCase("davisbase_columns")) {
			noColmns = 6;
			dataTypes.put(1, "int");
			dataTypes.put(2, "text");
			dataTypes.put(3, "text");
			dataTypes.put(4, "text");
			dataTypes.put(5, "int");
			dataTypes.put(6, "text");
		} else
			return;
		byte[] plDataType = new byte[noColmns - 1];
		String[] dataTypeStr = new String[noColmns];
		dataTypes.values().toArray(dataTypeStr);
		int payLoadSize = getPayloadSize(values, plDataType, dataTypeStr);
		payLoadSize += 6;

		// change offset calculation??
		boolean canInsert = canInsert(pageNo, payLoadSize);

		if (canInsert) {
			RecordCell cell = createCell(pageNo, fetchNextRowID(), (short) payLoadSize, plDataType, values);
			insertRec(pageNo, payLoadSize, cell);
		} else {
			int rowid = fetchNextRowID();
			int pNo = splitLeafPage(pageNo);
			RecordCell cell = createCell(pNo, rowid, (short) payLoadSize, plDataType, values);
			insertRec(pNo, payLoadSize, cell);
		}
	}

	public int splitLeafPage(int pageNo) throws IOException {
		// adding page
		int parent = findNonLeafNode(pageNo);
		if (parent == -1) {
			int pg = (int) (binaryFile.length() / FILESIZE) + 1;
			binaryFile.setLength(pg * FILESIZE);
			setNodeHeaders(pg);
			insertToNonLeaf(pg, pageNo);
			setAsNextPage(pg, pg + 1);

			binaryFile.setLength((pg + 1) * FILESIZE);
			setLeafHeaders(pg + 1);
			setAsNextPage(pageNo, pg + 1);
			return pg + 1;
		} else {
			int pg = (int) (binaryFile.length() / FILESIZE) + 1;
			insertToNonLeaf(parent, pageNo);
			setAsNextPage(parent, pg);
			setAsNextPage(pageNo, pg);
			return pg;
		}
	}

	public void insertToNonLeaf(int page, int pagePtr) throws IOException {
		if (isLeaf(page))
			return;

		int loc = fetchTopRecLoc(page) - 8;
		binaryFile.seek((page - 1) * FILESIZE + 2);
		binaryFile.writeShort(loc);

		// increment the count on file
		int count = findNoRecordsInPage(page);
		binaryFile.seek((page - 1) * FILESIZE + 1);
		binaryFile.writeByte(count + 1);

		int topRec = findTopRowId(pagePtr);
		binaryFile.seek((page - 1) * FILESIZE + loc);
		binaryFile.writeInt(pagePtr);
		binaryFile.writeInt(topRec);

		// add the pointer to the new rec to the pointer list.
		binaryFile.seek((page - 1) * FILESIZE + 8 + ((count == 0) ? 0 : ((count - 1) * 2)));
		binaryFile.writeShort(loc);
	}

	private void setAsNextPage(int currPage, int page) throws IOException {
		binaryFile.seek((currPage - 1) * FILESIZE + 4);
		binaryFile.writeInt(page);
	}

	private int findNonLeafNode(int page) throws IOException {
		int i = 1;
		int lastLeaf = fetchLastPage();
		while (i < lastLeaf) {
			if (isNode(i))
				return i;
			i++;
		}
		return -1;
	}

	public void updateToLeaf(String[] colNames, String[] data, int page, int location, int rowid) throws Exception {
		// +1 byte for number of columns, +6 bytes for the record headers, every column
		// other than rowid has a byte header
		Map<Integer, String> dataTypes = getColumnDataTypes();
		Map<Integer, ColumnDetails> columnDetails = getColumnDetails();
		Map<Integer, String> isNullable = getColumnNullable();
		Object[] nullables = isNullable.values().toArray();
		byte[] dataHeaders = new byte[columnDetails.size() - 1];
		int headerPointer = 0;

		int dataSize = (columnDetails.size() - 1) + 1 + 6;

		for (int i : columnDetails.keySet()) {
			ColumnDetails col = columnDetails.get(i);
			if (col.column_name.equalsIgnoreCase("rowid")) {
				continue;
			}
			int indx = findIndx(colNames, col.column_name);
			if (indx != -1) {
				dataSize += getDataTypeSize(col.data_type, data[indx].length());
				dataHeaders[headerPointer++] = (byte) getDataTypeHeader(col.data_type, false, data[indx].length());
			} else if (indx == -1 && col.is_nullable)
				dataHeaders[headerPointer++] = (byte) getDataTypeHeader(col.data_type, true, 0);
			else
				throw new Exception("Could not find column '" + col.column_name + "'");
		}

		// check leaf size
		byte[] plDataType = new byte[columnDetails.size() - 1];
		String[] dataTypeStr = new String[columnDetails.size()];
		dataTypes.values().toArray(dataTypeStr);
		int payLoadSize = getPayloadSize(data, plDataType, dataTypeStr);
		payLoadSize = payLoadSize + 6;

		boolean canInsert = canInsert(page, payLoadSize);

		if (canInsert) {
			RecordCell cell = createCell(page, rowid, (short) payLoadSize, plDataType, data);
			insertRec(page, payLoadSize, cell, location);
		} else {
			int pNo = splitLeafPage(page);
			RecordCell cell = createCell(pNo, rowid, (short) payLoadSize, plDataType, data);
			insertRec(pNo, payLoadSize, cell, location);
		}
	}

	private RecordCell createCell(int pageNo, int primaryKey, short payLoadSize, byte[] dataType, String[] values) {
		RecordCell cell = new RecordCell();
		cell.setPageNumber(pageNo);
		cell.setRowId(primaryKey);
		cell.setPayLoadSize(payLoadSize);

		PayLoad payload = new PayLoad();
		payload.setNumberOfColumns(Byte.parseByte(values.length - 1 + ""));
		payload.setDataTypes(dataType);
		payload.setData(values);

		cell.setPayload(payload);

		return cell;
	}

	private static int getPayloadSize(String[] values, byte[] plDataType, String[] dataType) throws Exception {

		int size = 1 + dataType.length - 1;
		for (int i = 1; i < values.length; i++) {
			// Trouble
			plDataType[i - 1] = (byte) getDataTypeHeader(dataType[i], false, values[i].length());
			size = size + getDataTypeSize(dataType[i], values[i].length());
		}

		return size;
	}

	private int findIndx(String[] columns, String column) {
		for (int i = 0; i < columns.length; i++) {
			if (column.trim().equalsIgnoreCase(columns[i].trim()))
				return i;
		}
		return -1;
	}

	private int fetchLastPage() throws IOException {
		int page = 0;
		int nextPage = 1;
		while (nextPage != 0xFFFFFFFF) {
			page = nextPage;
			nextPage = fetchNextLeafPage(page);
		}
		return page;
	}

	public int fetchNextRowID() throws IOException {
		int page = fetchLastPage();
		int noRecs = findNoRecordsInPage(page);
		if (noRecs == 0)
			return 1;
		binaryFile.seek(((page - 1) * FILESIZE) + (8 + ((noRecs - 1) * 2)));
		int lasRecLoc = binaryFile.readShort();
		binaryFile.seek(((page - 1) * FILESIZE) + lasRecLoc + 2);
		int recordValue = (int) binaryFile.readInt();
		int totRecs = totalNoRecords();
		return ((recordValue > totRecs) ? recordValue : totRecs) + 1;
	}

	public int totalNoRecords() throws IOException {
		int page = 1;
		int count = 0;
		while (page != 0xFFFFFFFF) {
			count += findNoRecordsInPage(page);
			page = fetchNextLeafPage(page);
		}
		return count;
	}

	public int findTopRowId(int page) throws IOException {
		int noRecs = findNoRecordsInPage(page);
		if (noRecs == 0)
			return 1;
		binaryFile.seek(((page - 1) * FILESIZE) + (8 + ((noRecs - 1) * 2)));
		int lasRecLoc = binaryFile.readShort();
		binaryFile.seek((page - 1) * FILESIZE + lasRecLoc + 2);
		return ((int) binaryFile.readInt());
	}

	public Map<Integer, RecordCell> selectRecords(String[] columnNames, String[] condition) throws Exception {
		Map<Integer, RecordCell> records = getAllData();

		if (condition.length > 0) {
			Map<Integer, RecordCell> filteredRecords = filterData(records, columnNames, condition);
			return filteredRecords;
		} else {
			return records;
		}
	}

	private Map<Integer, RecordCell> filterData(Map<Integer, RecordCell> records, String[] resultColumns,
			String[] condns) throws Exception {
		Map<Integer, RecordCell> filteredRecords = new LinkedHashMap<>();
		Map<Integer, String> colNames = getColumnNames();

		int whereOrdlPos = 2;
		for (Map.Entry<Integer, String> entry : colNames.entrySet()) {
			String columnName = entry.getValue();
			if (columnName.equals(condns[0])) {
				whereOrdlPos = entry.getKey();
			}
		}

		for (Map.Entry<Integer, RecordCell> entry : records.entrySet()) {
			RecordCell cell = entry.getValue();
			PayLoad payload = cell.getPayload();
			String[] data = payload.getData();
			byte[] dataTypeCodes = payload.getDataTypes();

			boolean result;

			if (whereOrdlPos == 1 && !IsMetaTable)
				result = checkData((byte) 0x06, String.valueOf(cell.getRowId()), condns);
			else
				result = checkData(dataTypeCodes[whereOrdlPos - 2], data[whereOrdlPos - 2], condns);

			if (result)
				filteredRecords.put(entry.getKey(), entry.getValue());
		}

		return filteredRecords;
	}

	private Map<Integer, RecordCell> getAllData() throws Exception {
		int page = 0;
		int nextPage = 1;
		Map<Integer, RecordCell> dataRecs = new LinkedHashMap<>();
		while (nextPage != 0xFFFFFFFF) {
			page = nextPage;
			dataRecs.putAll(getData(page));
			nextPage = fetchNextLeafPage(page);
		}
		return dataRecs;
	}

	private Map<Integer, RecordCell> getData(int page) throws Exception {
		Map<Integer, RecordCell> dataRecs = new LinkedHashMap<>();
		short[] pointerList = fetchRecordPointers(page);
		for (short recLoc : pointerList) {
			PayLoad payLoad = new PayLoad();

			binaryFile.seek((page - 1) * FILESIZE + recLoc);
			payLoad.setSize(binaryFile.readShort());

			int rowId = binaryFile.readInt();

			payLoad.setNumberOfColumns(binaryFile.readByte());

			byte[] dataTypes = new byte[payLoad.getNoColumns()];

			// for (int i = 0; i < payLoad.getNoColumns(); i++) {
			// dataTypes[i] = binaryFile.readByte();
			// }
			binaryFile.read(dataTypes, 0, payLoad.getNoColumns());

			payLoad.setDataTypes(dataTypes);

			String[] dataArray = new String[payLoad.getNoColumns()];
			for (int i = 0; i < payLoad.getNoColumns(); i++) {
				byte head = dataTypes[i];
				int dataSize = getSizeByHeader(head);
				// byte[] data = new byte[dataSize];
				// binaryFile.read(data, 0, dataSize);
				//
				// dataArray[i] = new String(data);
				switch (head) {
				case 0x00:
					dataArray[i] = Integer.toString(binaryFile.readByte());
					dataArray[i] = "null";
					break;

				case 0x01:
					dataArray[i] = Integer.toString(binaryFile.readShort());
					dataArray[i] = "null";
					break;

				case 0x02:
					dataArray[i] = Integer.toString(binaryFile.readInt());
					dataArray[i] = "null";
					break;

				case 0x03:
					dataArray[i] = Long.toString(binaryFile.readLong());
					dataArray[i] = "null";
					break;

				case 0x04:
					dataArray[i] = Integer.toString(binaryFile.readByte());
					break;

				case 0x05:
					dataArray[i] = Integer.toString(binaryFile.readShort());
					break;

				case 0x06:
					dataArray[i] = Integer.toString(binaryFile.readInt());
					break;

				case 0x07:
					dataArray[i] = Long.toString(binaryFile.readLong());
					break;

				case 0x08:
					dataArray[i] = String.valueOf(binaryFile.readFloat());
					break;

				case 0x09:
					dataArray[i] = String.valueOf(binaryFile.readDouble());
					break;

				case 0x0A:
					long tmp = binaryFile.readLong();
					Date dateTime = new Date(tmp);
					DateTimeFormatter formater = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
					dataArray[i] = formater.format((TemporalAccessor) dateTime);
					break;

				case 0x0B:
					long tmp1 = binaryFile.readLong();
					Date date = new Date(tmp1);
					DateTimeFormatter formater1 = DateTimeFormatter.ofPattern("yyyy-MM-dd");
					dataArray[i] = formater1.format((TemporalAccessor) date);
					break;

				default:
					int len = dataSize;
					byte[] bytes = new byte[len];
					binaryFile.read(bytes, 0, len);
					dataArray[i] = new String(bytes);
					break;
				}
			}
			payLoad.setData(dataArray);

			RecordCell record = new RecordCell(recLoc, page);
			record.setPayLoadSize(payLoad.getSize());
			record.setPayload(payLoad);
			record.setRowId(rowId);

			dataRecs.put(rowId, record);
		}
		return dataRecs;
	}

	private short[] fetchRecordPointers(int page) throws IOException {
		int noRecs = findNoRecordsInPage(page);
		short[] recPointers = new short[noRecs];
		binaryFile.seek((page - 1) * FILESIZE + 8);
		for (int i = 0; i < noRecs; i++)
			recPointers[i] = binaryFile.readShort();
		return recPointers;
	}

	public boolean checkData(byte headerCode, String data, String[] conds) throws Exception {
		if (!isNullHeader(headerCode)) {
			if (headerCode >= 0x04 && headerCode <= 0x07) {
				Long longData = Long.parseLong(data);
				switch (conds[1]) {
				case "=":
					if (longData == Long.parseLong(conds[2]))
						return true;
					break;
				case ">":
					if (longData > Long.parseLong(conds[2]))
						return true;
					break;
				case "<":
					if (longData < Long.parseLong(conds[2]))
						return true;
					break;
				case "<=":
					if (longData <= Long.parseLong(conds[2]))
						return true;
					break;
				case ">=":
					if (longData >= Long.parseLong(conds[2]))
						return true;
					break;
				case "<>":
					if (longData != Long.parseLong(conds[2]))
						return true;
					break;
				default:
					throw new Exception("Unknown comparision operation '" + conds[2] + "'.");
				}
			} else if (headerCode == 0x08 || headerCode == 0x09) {
				Double doubleData = Double.parseDouble(data);
				switch (conds[1]) {
				case "=":
					if (doubleData == Double.parseDouble(conds[2]))
						return true;
					break;
				case ">":
					if (doubleData > Double.parseDouble(conds[2]))
						return true;
					break;
				case "<":
					if (doubleData < Double.parseDouble(conds[2]))
						return true;
					break;
				case "<=":
					if (doubleData <= Double.parseDouble(conds[2]))
						return true;
					break;
				case ">=":
					if (doubleData >= Double.parseDouble(conds[2]))
						return true;
					break;
				case "<>":
					if (doubleData != Double.parseDouble(conds[2]))
						return true;
					break;
				default:
					throw new Exception("Unknown comparision operation '" + conds[2] + "'.");
				}
			} else if (headerCode >= 0x0C) {

				conds[2] = conds[2].replaceAll("'", "");
				conds[2] = conds[2].replaceAll("\"", "");
				switch (conds[1]) {
				case "=":
					if (data.equalsIgnoreCase(conds[2]))
						return true;
					break;
				case "<>":
					if (!data.equalsIgnoreCase(conds[2]))
						return true;
					break;
				default:
					System.out.println("undefined operator return false");
					return false;
				}
			}
		}
		return false;
	}

	private static int getDataTypeSize(String dataType, int length) {
		int size = 0;
		switch (dataType.trim()) {
		case "tinyint": {
			size = 1;
			break;
		}
		case "smallint": {
			size = 2;
			break;
		}
		case "real":
		case "int": {
			size = 4;
			break;
		}

		case "double":
		case "datetime":
		case "date":
		case "bigint": {
			size = 8;
			break;
		}
		default: {
			size = length;
		}
		}
		return size;
	}

	private static int getDataTypeHeader(String dataType, boolean isNull, int length) throws UnknownDataTypeException {
		switch (dataType) {
		case "tinyint": {
			return isNull ? 0x00 : 0x04;
		}
		case "smallint": {
			return isNull ? 0x01 : 0x05;
		}
		case "int": {
			return isNull ? 0x02 : 0x06;
		}
		case "bigint": {
			return isNull ? 0x03 : 0x07;
		}
		case "real": {
			return isNull ? 0x02 : 0x08;
		}
		case "double": {
			return isNull ? 0x03 : 0x09;
		}
		case "datetime": {
			return isNull ? 0x03 : 0x0A;
		}
		case "date": {
			return isNull ? 0x03 : 0x0B;
		}
		case "text": {
			return 0x0C + length;
		}
		default:
			throw new UnknownDataTypeException(dataType);
		}
	}

	private static int getSizeByHeader(int header) throws UnknownDataTypeException {
		switch (header) {
		case 0x00:
		case 0x04: {
			return 1;
		}
		case 0x01:
		case 0x05: {
			return 2;
		}
		case 0x02:
		case 0x06:
		case 0x08: {
			return 4;
		}

		case 0x03:
		case 0x07:
		case 0x09:
		case 0x0A:
		case 0x0B: {
			return 8;
		}
		default:
			if (header >= 0x0C)
				return header - 0x0C;
			else
				throw new UnknownDataTypeException(header + "");
		}
	}

	private static boolean isNullHeader(int header) {
		return header >= 0x00 && header <= 0x03;
	}

	public void closeFile() throws IOException {
		binaryFile.close();
	}

	public void deleteRec(int pageNumber, short location) throws IOException {
		short[] pointers = fetchRecordPointers(pageNumber);
		int recCount = findNoRecordsInPage(pageNumber);
		int index = findIndex(pointers, location);
		if (index != -1) {
			binaryFile.seek((pageNumber - 1) * FILESIZE + 8 + (index * 2));
			for (int i = index + 1; i < pointers.length; i++) {
				binaryFile.writeShort(pointers[i]);
			}
			binaryFile.writeShort(0);
			setRecordCount(pageNumber, recCount - 1);
		}
	}

	private void setRecordCount(int page, int count) throws IOException {
		binaryFile.seek((page - 1) * FILESIZE + 1);
		binaryFile.writeByte(count);
	}

	private int findIndex(short[] array, short item) {
		for (int i = 0; i < array.length; i++)
			if (array[i] == item)
				return i;
		return -1;
	}
}
