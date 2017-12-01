package myhive;

import org.apache.hadoop.hive.cli.CliDriver;

public class HiveDriver {

	public static void main(String[] args) {
		int ret = 0;
		try {
			//System.setProperty("jline.WindowsTerminal.directConsole","false");
			ret = new CliDriver().run(args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(ret);
	}
}
