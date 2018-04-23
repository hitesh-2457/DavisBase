import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

public class Operations {

	public static void createTable(String path, String tableName, String[] columns) throws Exception {
		try {
			insertToMetaTables(tableName);
			for (int i = 0; i < columns.length; i++) {
				String[] data = columns[i].trim().split(" ");
				String[] columnMeta = new String[6];
				columnMeta[0] = "0";
				columnMeta[1] = tableName;
				columnMeta[2] = data[0];
				columnMeta[3] = data[1];
				columnMeta[4] = String.valueOf(i + 1);
				if (data.length == 3 && data[2].equalsIgnoreCase("null"))
					columnMeta[5] = "YES";
				else
					columnMeta[5] = "NO";
				insertToMetaColumns(columnMeta);
			}

		} catch (Exception e) {
			System.out.println("Failed to create Table.");
		}
	}

	private static void insertToMetaTables(String tableName) throws Exception {
		String path = "data/catalog";
		Table metaTable = new Table();
		metaTable.initialize(path, "davisbase_tables");
		metaTable.insertToMeta(new String[] { "rowid", "tableName" },
				new String[] { String.valueOf(metaTable.fetchNextRowID()), tableName });
	}

	private static void insertToMetaColumns(String[] values) throws Exception {
		String path = "data/catalog";
		Table metaCol = new Table();
		metaCol.initialize(path, "davisbase_columns");
		metaCol.insertToMeta(
				new String[] { "rowid", "tableName", "column_name", "data_type", "ordinal_position", "is_nullable" },
				values);
	}

	public static void makeFiles(String path, String table) throws IOException {
		Table metaTable = new Table();
		metaTable.createFile(path, table);
		metaTable.closeFile();
	}

	private static boolean tableExists(String path, String tableName) {
		File tableFile = new File(path + "/" + tableName + ".tbl");
		return tableFile.exists();
	}

	public static void validateCatalogueTables() throws Exception {
		File catalogPath = new File("data/catalog");
		catalogPath.mkdirs();
		File dataPath = new File("data/user_data");
		dataPath.mkdirs();

		if (!Operations.tableExists("data/catalog", "davisbase_tables")
				&& !Operations.tableExists("data/catalog", "davisbase_columns")) {
			makeFiles("data/catalog", "davisbase_tables");

			makeFiles("data/catalog", "davisbase_columns");

			Operations.createTable("data/catalog", "davisbase_tables", new String[] { "rowid int", "table_name text" });
			Operations.createTable("data/catalog", "davisbase_columns", new String[] { "rowid int", "table_name text",
					"column_name text", "data_type text", "ordinal_position int", "is_nullable text" });
		}
	}

	public static void insert(String path, String tableName, String[] columnNames, String[] values) throws Exception {
		Table table = new Table();
		table.initialize(path, tableName);
		table.insertToLeaf(columnNames, values);
		System.out.println("Successfully inserted the record.");
	}

	public static void select(String path, String tableName, String[] columnNames, String[] condition)
			throws Exception {
		File file = new File(path + "/" + tableName + ".tbl");
		if (!file.exists()) {
			System.out.println(tableName + " Table does not exist.");
			return;
		}
		Table table = new Table();
		table.initialize(path, tableName);
		Map<Integer, RecordCell> data = table.selectRecords(columnNames, condition);

		Set<Entry<Integer, RecordCell>> dataSet = data.entrySet();

		List<String> colmns = new ArrayList<>();
		colmns.addAll(table.getColumnNames().values());

		StringBuffer colNames = new StringBuffer();
		if (columnNames.length == 1 && columnNames[0].trim().equalsIgnoreCase("*")) {
			columnNames = new String[colmns.size()];
			for (int i = 0; i < colmns.size(); i++) {
				colNames.append(colmns.get(i) + " | ");
				columnNames[i] = colmns.get(i);
			}
		} else {
			for (String col : colmns)
				if (arrayContains(columnNames, col))
					colNames.append(col + " | ");
		}
		System.out.println(colNames.toString());

		int count = 0;
		for (Map.Entry<Integer, RecordCell> entry : dataSet) {
			RecordCell cellRecord = entry.getValue();
			PayLoad cellPayLoad = cellRecord.getPayload();

			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < colmns.size(); i++)
				if (arrayContains(columnNames, colmns.get(i)))
					if (i == 0)
						sb.append(cellRecord.getRowId() + " | ");
					else
						sb.append(cellPayLoad.getData()[i - 1] + " | ");
			System.out.println(sb.toString());
			count++;
		}
		System.out.println("\nFound total of " + count + " records.");
	}

	public static boolean arrayContains(String[] array, String item) {
		for (String ele : array)
			if (item.trim().equalsIgnoreCase(ele.trim()))
				return true;
		return false;
	}

	public static void update(String path, String tableName, String[] data, String[] condition) throws Exception {
		Table table = new Table();
		table.initialize(path, tableName);
		Map<Integer, RecordCell> filteredData = table.selectRecords(new String[] { "*" }, condition);
		Map<Integer, String> columns = table.getColumnNames();
		int total = filteredData.size();

		for (Map.Entry<Integer, RecordCell> entry : filteredData.entrySet()) {
			RecordCell rec = entry.getValue();
			PayLoad payLoad = rec.getPayload();
			String[] colNames = columns.values().toArray(new String[0]);
			int rowid = rec.getRowId();

			String[] payLoadData = new String[payLoad.getData().length + 1];
			payLoadData[0] = String.valueOf(rowid);
			for (int i = 0; i < payLoad.getData().length; i++)
				payLoadData[i + 1] = payLoad.getData()[i];

			int index = findIndx(colNames, data[0]);
			payLoadData[index] = data[2];
			table.updateToLeaf(colNames, payLoadData, rec.getPageNumber(), rec.getLocation(), rowid);
		}
		System.out.println("Total of " + total + " records were updated.");
	}

	private static int findIndx(String[] columns, String column) {
		for (int i = 0; i < columns.length; i++) {
			if (column.trim().equalsIgnoreCase(columns[i].trim()))
				return i;
		}
		return -1;
	}

	public static void delete(String path, String tableName, String[] condition) throws Exception {
		Table table = new Table();
		table.initialize(path, tableName);
		Map<Integer, RecordCell> filteredData = table.selectRecords(new String[] { "*" }, condition);
		int total = filteredData.size();

		for (Map.Entry<Integer, RecordCell> entry : filteredData.entrySet()) {
			RecordCell rec = entry.getValue();
			table.deleteRec(rec.getPageNumber(), rec.getLocation());
		}
		System.out.println("Total of " + total + " records were deleted.");
	}

	public static void dropTable(String path, String tableName) throws Exception {
		delete("data/catalog", "davisbase_columns", new String[] { "table_name", "=", tableName });
		delete("data/catalog", "davisbase_tables", new String[] { "table_name", "=", tableName });

		File file = new File(path + "/" + tableName + ".tbl");
		if (!file.delete())
			System.out.println("The table is successfully removed from Meta, but could not be delete from FileSystem.");
		else
			System.out.println("Dropped table " + tableName + " successfully.");
	}
}
