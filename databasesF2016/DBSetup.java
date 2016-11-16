package databasesF2016;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.*;
import java.util.*;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

public class DBSetup {
    // CSV stuff.
    String pathToCSV = null;
	Reader CSVReader = null;
    // Oracle stuff.
    Connection connection;
    Statement statement;


    public DBSetup(String pathToCSV) {
        if (!pathToCSV.equals(null)) {
			this.pathToCSV = pathToCSV;
		} else {
			System.err.println("Received invalid CSV path.");
		}

        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch(Exception e) {
            System.out.println(e);
        }
    }

    // Always call this to establish a connection to the Oracle database.
    public void openConnection() {
        try {
            connection = DriverManager.getConnection(
                "jdbc:oracle:thin:@localhost:1521:xe", "username", "password");
            statement = connection.createStatement();
        } catch(Exception e) {
            System.out.println(e);
        }
    }

    // Always call this when done to close the connection to the database.
    public void closeConnection() {
        try {
            connection.close();
        } catch(Exception e) {
            System.out.println(e);
        }
    }

    // Called to create the various tables in the DB.
    public void tableSetup() {
        try {
            statement.execute("CREATE TABLE Event_Clearance_Group(name varchar(80) not null, primary key (name))");
            System.out.println("Event-Clearance-Group table created.");

            statement.execute("CREATE TABLE Type(event_clearance_code integer not null, event_clearance_description varchar(250) not null, group_name varchar(80), foreign key (group_name) references Event_Clearance_Group(name), primary key (event_clearance_code))");
            System.out.println("Type table created.");

            statement.execute("CREATE TABLE District(letter char(1) not null, primary key (letter))");
            System.out.println("District table created.");

            statement.execute("CREATE TABLE Zone(num integer not null, d_letter char(1), foreign key (d_letter) references District(letter), primary key (d_letter, num))");
            System.out.println("Zone table created.");

            statement.execute("CREATE TABLE Location(longitude decimal(9, 5) not null, latitude decimal(9, 5) not null, address varchar(200), census_tract_number varchar(20), district_letter char(1), zone_number integer, foreign key (district_letter, zone_number) references Zone(d_letter, num), primary key (address))");
            System.out.println("Location table created.");

            statement.execute("CREATE TABLE Incident(general_offense_number integer not null, event_code integer, addr varchar(200), foreign key (event_code) references Type(event_clearance_code), foreign key (addr) references Location(address), primary key (general_offense_number))");
            System.out.println("Incident table created.");

            statement.execute("CREATE TABLE CrimeTime(crime_date date not null, time varchar(20) not null, offense_number integer, foreign key (offense_number) references Incident(general_offense_number), primary key (crime_date, time))");
            System.out.println("CrimeTime table created.");
        } catch(Exception e) {
            System.out.println(e);
        }
    }

    // Deletes all of the tables for the project. Used if the program
    // needs to be rerun for some reason, such as in debugging.
    public void dropTables() {
        try {
            statement.execute("Drop table CrimeTime");
            System.out.println("CrimeTime table dropped.");

            statement.execute("Drop table Incident");
            System.out.println("Incident table dropped.");

            statement.execute("Drop table Location");
            System.out.println("Location table dropped.");

            statement.execute("Drop table Zone");
            System.out.println("Zone table dropped.");

            statement.execute("Drop table District");
            System.out.println("District table dropped.");

            statement.execute("Drop table Type");
            System.out.println("Type table dropped.");

            statement.execute("Drop table Event_Clearance_Group");
            System.out.println("Event_Clearance_Group table dropped.");
        } catch(Exception e) {
            System.out.println(e);
        }
    }

    // Open the CSV file and store the rows in memory.
	public void openFile() throws IOException {
		// Create a File object using the string path from the constructor.
		File CSVFile = new File(pathToCSV);
		this.CSVReader = new FileReader(CSVFile);

	}

    // Add the data from the CSV to the DB.
	public void addData() throws IOException {

		Iterable<CSVRecord> rows = CSVFormat.DEFAULT.parse(CSVReader);
        int rowNumber = 1;

		for (CSVRecord row : rows) {
            float percent = rowNumber % 100;
            if(percent == 0) { System.out.print(Math.floor((rowNumber / 1328870.0) * 100) + "% done, " + rowNumber + " rows parsed\r"); }

            String name = "";
            String event_clearance_code = "";
            String event_clearance_description = "";
            String letter = "";
            String num = "";
            String longitude = "";
            String latitude = "";
            String address = "";
            String census_tract_number = "";
            String general_offense_number = "";
            String crime_date = "";
            String time = "";

			// Go through each cell in the row and assign it to the proper variable.
			for (int i = 0; i < row.size(); ++i) {
				String cellText = row.get(i);
				if (cellText.contains(",")) {
					cellText = cellText.replace(",", " : ");
				}

                // Assign the value in the cell to a variable.
                switch(i) {
                    case 2:     if(cellText.length() > 0) { general_offense_number = cellText; }
                                break;
                    case 3:     if(cellText.length() > 0) { event_clearance_code = cellText; }
                                break;
                    case 4:     if(cellText.length() > 0) { event_clearance_description = cellText; }
                                break;
                    case 6:     if(cellText.length() > 0) { name = cellText; }
                                break;
                    case 7:     if(cellText.length() > 9) { crime_date = cellText.substring(0, 10); }
                                if(cellText.length() > 21) { time = cellText.substring(11, 22); }
                                break;
                    case 8:     if(cellText.length() > 0) { address = cellText; }
                                break;
                    case 9:     if(cellText.length() == 1) { letter = cellText; }
                                break;
                    case 10:    if(cellText.length() == 2) { num = cellText.substring(1, 2); }
                                break;
                    case 11:    if(cellText.length() > 0) { census_tract_number = cellText; }
                                break;
                    case 12:    int len1 = cellText.length();
                                if(len1 >= 9) { len1 = 9; }
                                longitude = cellText.substring(0, len1);
                                break;
                    case 13:    int len2 = cellText.length();
                                if(len2 >= 9) { len2 = 9; }
                                latitude = cellText.substring(0, len2);
                                break;
                    default:    break;
                }
			}

            // Add the data from this row to the database.
            if(rowNumber > 1) {
                if(!name.isEmpty()) {
                    try {
                        statement.execute("Insert into Event_Clearance_Group values('" + name + "')");
                    } catch(Exception e) {}
                    if(!event_clearance_code.isEmpty()) {
                        try {
                            statement.execute("Insert into Type values(" + event_clearance_code + ", '" + event_clearance_description + "', '" + name + "')");
                        } catch(Exception e) {}
                    }
                }
                if(!letter.isEmpty()) {
                    try {
                        statement.execute("Insert into District values('" + letter + "')");
                    } catch(Exception e) {}
                    if(!num.isEmpty()) {
                        try {
                            statement.execute("Insert into Zone values('" + num + "', '" + letter + "')");
                        } catch(Exception e) {}
                    }
                }
                if(!address.isEmpty()) {
                    try {
                        statement.execute("Insert into Location values(" + longitude + ", " + latitude + ", '" + address + "', '" + census_tract_number + "', '" + letter + "', '" + num + "')");
                    } catch(Exception e) {}
                    if(!general_offense_number.isEmpty() && !event_clearance_code.isEmpty()) {
                        try {
                            statement.execute("Insert into Incident values(" + general_offense_number + ", " + event_clearance_code + ", '" + address +  "')");
                        } catch(Exception e) {}
                    }
                }
                if(!crime_date.isEmpty() && !time.isEmpty()) {
                    try {
                        statement.execute("Insert into CrimeTime values(TO_DATE('" + crime_date + "', 'mm/dd/yyyy'), '" + time + "', '" + general_offense_number + "')");
                    } catch(Exception e) {}
                }
                if(percent == 0) {
                    try {
                        statement.execute("Commit");
                    } catch(Exception e) {}
                }
            }

			++rowNumber;
		}

		// Clean up stream.
		if (CSVReader != null) {
			CSVReader.close();
		}
	}
}
