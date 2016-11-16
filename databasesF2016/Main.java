package databasesF2016;

public class Main {

	public static void main(String[] args) {
		DBSetup seattlePoliceReportHandler = new DBSetup("/home/ryan/Documents/Databases/Seattle_Police_Department_911_Incident_Response.csv");
		try {
			seattlePoliceReportHandler.openConnection();
			seattlePoliceReportHandler.dropTables();
			seattlePoliceReportHandler.tableSetup();
			seattlePoliceReportHandler.openFile();
			seattlePoliceReportHandler.addData();
			seattlePoliceReportHandler.closeConnection();
		} catch (Exception e) {
			System.err.println(e.toString());
		}
	}

}
