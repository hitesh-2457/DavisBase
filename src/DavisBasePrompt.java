import java.util.Scanner;
import java.util.ArrayList;
import java.util.Arrays;

import static java.lang.System.out;

/**
 * @author Chris Irwin Davis
 * @version 1.0 <b>
 *          <p>
 *          This is an example of how to create an interactive prompt
 *          </p>
 *          <p>
 *          There is also some guidance to get started wiht read/write of binary
 *          data files using RandomAccessFile class
 *          </p>
 *          </b>
 */
public class DavisBasePrompt {

	/* This can be changed to whatever you like */
	static String prompt = "davisql> ";
	static String version = "v1.0";
	static String copyright = "©2018 Hitesh Gupta";
	static boolean isExit = false;
	static String path = "data/user_data";

	/* Page size for all files is 512 bytes by default. */
	static long pageSize = 512;

	/*
	 * The Scanner class is used to collect user commands from the prompt There are
	 * many ways to do this. This is just one.
	 *
	 * Each time the semicolon (;) delimiter is entered, the userCommand String is
	 * re-populated.
	 */
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");

	/**
	 * ********************************************************************** Main
	 * method
	 */
	public static void main(String[] args) {

		try {
			/* Display the welcome screen */
			splashScreen();

			/* Initialize the DataBase */
			initialize();

			/* Variable to collect user input from the prompt */
			String userCommand = "";

			while (!isExit) {
				System.out.print(prompt);
				/* toLowerCase() renders command case insensitive */
				userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
				// userCommand = userCommand.replace("\n", "").replace("\r", "");
				parseUserCommand(userCommand);
			}
			System.out.println("Exiting...");
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}

	}

	/**
	 * ***********************************************************************
	 * Static method definitions
	 */

	/**
	 * Initialize Database. Create missing files
	 */
	public static void initialize() throws Exception {
		Operations.validateCatalogueTables();
	}

	/**
	 * Display the splash screen
	 */
	public static void splashScreen() {
		System.out.println(line("-", 80));
		System.out.println("Welcome to DavisBaseLite"); // Display the string.
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(line("-", 80));
	}

	/**
	 * @param s
	 *            The String to be repeated
	 * @param num
	 *            The number of time to repeat String s.
	 * @return String A String object, which is the String s appended to itself num
	 *         times.
	 */
	public static String line(String s, int num) {
		String a = "";
		for (int i = 0; i < num; i++) {
			a += s;
		}
		return a;
	}

	public static void printCmd(String s) {
		System.out.println("\n\t" + s + "\n");
	}

	public static void printDef(String s) {
		System.out.println("\t\t" + s);
	}

	/**
	 * Help: Display supported commands
	 */
	public static void help() {
		out.println(line("*", 80));
		out.println("SUPPORTED COMMANDS\n");
		out.println("All commands below are case insensitive\n");
		out.println("SHOW TABLES;");
		out.println("\tDisplay the names of all tables.\n");
		// printCmd("SELECT * FROM <table_name>;");
		// printDef("Display all records in the table <table_name>.");
		out.println("SELECT <column_list> FROM <table_name> [WHERE <condition>];");
		out.println("\tDisplay table records whose optional <condition>");
		out.println("\tis <column_name> = <value>.\n");
		out.println("DROP TABLE <table_name>;");
		out.println("\tRemove table data (i.e. all records) and its schema.\n");
		out.println("UPDATE TABLE <table_name> SET <column_name> = <value> [WHERE <condition>];");
		out.println("\tModify records data whose optional <condition> is\n");
		out.println("VERSION;");
		out.println("\tDisplay the program version.\n");
		out.println("HELP;");
		out.println("\tDisplay this help information.\n");
		out.println("EXIT;");
		out.println("\tExit the program.\n");
		out.println(line("*", 80));
	}

	/**
	 * return the DavisBase version
	 */
	public static String getVersion() {
		return version;
	}

	public static String getCopyright() {
		return copyright;
	}

	public static void displayVersion() {
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
	}

	public static void parseUserCommand(String userCommand) {

		/*
		 * commandTokens is an array of Strings that contains one token per array
		 * element The first token can be used to determine the type of command The
		 * other tokens can be used to pass relevant parameters to each command-specific
		 * method inside each case statement
		 */
		// String[] commandTokens = userCommand.split(" ");
		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));

		/*
		 * This switch handles a very small list of hardcoded commands of known syntax.
		 * You will want to rewrite this method to interpret more complex commands.
		 */
		switch (commandTokens.get(0)) {
		case "select":
//			System.out.println("CASE: SELECT");
			parseQuery(userCommand);
			break;
		case "drop":
//			System.out.println("CASE: DROP");
			dropTable(userCommand);
			break;
		case "create":
//			System.out.println("CASE: CREATE");
			parseCreateTable(userCommand);
			break;
		case "update":
//			System.out.println("CASE: UPDATE");
			parseUpdate(userCommand);
			break;
		case "insert":
//			System.out.println("CASE: INSERT");
			parseInsert(userCommand);
			break;
		case "delete":
//			System.out.println("CASE: DELETE");
			parseDelete(userCommand);
			break;
		case "show":
//			System.out.println("CASE: SHOW");
			parseShow(userCommand);
			break;
		case "help":
			help();
			break;
		case "version":
			displayVersion();
			break;
		case "exit":
			isExit = true;
			break;
		case "quit":
			isExit = true;
		default:
			System.out.println("I didn't understand the command: \"" + userCommand + "\"");
			break;
		}
	}

	private static void parseDelete(String userCommand) {
		// DELETE FROM table_name [WHERE condition];
		String[] splitOnWhere = userCommand.split("where");
		if (splitOnWhere.length < 2) {
			System.out.println("Missing Where clause.");
			return;
		}
		String[] condition = splitOnWhere[1].trim().split(" ");
		String[] queryString = splitOnWhere[0].trim().split(" ");
		String tableName = queryString[queryString.length - 1];
		try {
			Operations.delete(path, tableName, condition);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void parseShow(String userCommand) {
		// SHOW TABLES;
		if (userCommand.split(" ")[1].trim().equalsIgnoreCase("tables")) {
			String[] columnNames = new String[] { "*" };
			String[] condition = new String[0];
			try {
				Operations.select("data/catalog", "davisbase_tables", columnNames, condition);
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else
			System.out.println("Unknown command: " + userCommand);
	}

	/**
	 * Stub method for dropping tables
	 *
	 * @param dropTableString
	 *            is a String of the user input
	 */
	public static void dropTable(String dropTableString) {
		// DROP TABLE table_name;
		String[] tokens = dropTableString.split(" ");
		try {
			Operations.dropTable(path, tokens[tokens.length - 1].trim());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Stub method for executing queries
	 *
	 * @param queryString
	 *            is a String of the user input
	 */
	public static void parseQuery(String queryString) {
		// SELECT [col_names] FROM table_name [WHERE condition];
		String[] splitOnWhere = queryString.split("where");
		String querySplit[] = splitOnWhere[0].trim().split(" ");
		String tableName = querySplit[querySplit.length - 1];

		String[] cols = splitOnWhere[0].trim().split("from")[0].trim().replace("select", "").split(",");
		String[] columnNames = new String[cols.length];
		for (int i = 0; i < cols.length; i++)
			columnNames[i] = cols[i].trim();

		String[] condition = new String[0];
		if (splitOnWhere.length > 1) {
			condition = splitOnWhere[1].trim().split(" ");
		}
		try {
			if (tableName.contains("davisbase"))
				Operations.select("data/catalog", tableName, columnNames, condition);
			else
				Operations.select(path, tableName, columnNames, condition);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Stub method for updating records
	 *
	 * @param updateString
	 *            is a String of the user input
	 */
	public static void parseUpdate(String updateString) {
		// UPDATE table_name SET column_name = value [WHERE condition]
		String[] splitOnWhere = updateString.split("where");
		if (splitOnWhere.length < 2) {
			System.out.println("Missing where clause. Can not update without where claues.");
			return;
		}
		String[] condition = splitOnWhere[1].trim().split(" ");
		String[] splitOnSet = splitOnWhere[0].trim().split("set");
		String tableName = splitOnSet[0].trim().split(" ")[1];
		String[] data = splitOnSet[1].trim().split(" ");

		if (tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns")) {
			System.out.println("can not update Meta tables.");
			return;
		}
		try {
			Operations.update(path, tableName, data, condition);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Stub method for updating records
	 *
	 * @param insertString
	 *            is a String of the user input
	 */
	public static void parseInsert(String insertString) {
		// INSERT INTO table_name [column_list] VALUES value_list
		ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(insertString.split(" ")));

		/* Define table file name */
		String tableName = createTableTokens.get(2);
		String[] temp = insertString.replaceAll("\\(", " ").replaceAll("\\)", " ").split("values");
		String[] values = temp[1].trim().split(",");
		String[] temp2 = temp[0].trim().split(tableName);
		String[] columnNames = temp2[1].trim().split(",");

		try {
			for (int i = 0; i < values.length; i++)
				values[i] = values[i].trim();

			for (int i = 0; i < columnNames.length; i++)
				columnNames[i] = columnNames[i].trim();

			Operations.insert(path, tableName, columnNames, values);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Stub method for creating new tables
	 *
	 * @param createTableString
	 *            is a String of the user input
	 */
	public static void parseCreateTable(String createTableString) {
		// CREATE TABLE table_name ( row_id INT, column_name2 data_type2 [NOT NULL],
		// column_name3 data_type3 [NOT NULL], ...)
		ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(createTableString.split(" ")));

		/* Define table file name */
		String tableName = createTableTokens.get(2);

		/* YOUR CODE GOES HERE */
		String[] temp = createTableString.replaceAll("\\(", " ").replaceAll("\\)", " ").split(tableName);
		String[] columnNames = temp[1].trim().split(",");

		try {
			if (columnNames.length < 1)
				throw new Exception("Can not create a table with no columns.");
			String[] col1_Details = columnNames[0].split(" ");
			if(!col1_Details[0].trim().equalsIgnoreCase("rowid") || col1_Details[col1_Details.length-1].trim().equalsIgnoreCase("NULL"))
				throw new Exception("Primary column should be named 'rowid' and cannot be 'NULL'");

			/* Code to create a .tbl file to contain table data */
			Operations.makeFiles(path, tableName);

			/*
			 * Code to insert a row in the davisbase_tables table i.e. database catalog
			 * meta-data
			 */

			/*
			 * Code to insert rows in the davisbase_columns table for each column in the new
			 * table i.e. database catalog meta-data
			 */
			Operations.createTable(path, tableName, columnNames);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}