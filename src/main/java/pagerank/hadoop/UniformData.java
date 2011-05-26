package pagerank.hadoop;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

public class UniformData {
	public static final String resultFilename = "pagerank/graphdata";
	public static final String[] inputFiles = {"data/favorite_resume", "data/favorite_vacancy", "data/resume_target_employer", "data/resume_user", "data/vacancy_employer", "data/vacancy_invitation", "data/vacancy_response"};
	public static final String[][] columnNames = {{"r","e"}, {"v","u"}, {"r","e"}, {"r","u"}, {"v","e"}, {"v","r"}, {"r","v"}};
	public static final boolean[] reverce = {true, true, false, false, false, false, false};

	private static String readLine(FSDataInputStream in)
	{
		int len = 0;
		byte[] input = new byte[1000];
		
		while(true) {
			try {
				in.read(input, len, 1);
				if(input[len] == '\n')
					break;
				len++;
			} catch (IOException e) {
				break;
			}
		}
		
		if(len > 0) {
			return new String(input);
		}
		else
			return null;
	}
	
	public static void main (String [] args) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		Path filenamePath = new Path(resultFilename);

		try {
			if (fs.exists(filenamePath)) {
				fs.delete(filenamePath, true);
			}
			
			FSDataOutputStream out = fs.create(filenamePath);

			for(int k = 0; k < inputFiles.length; k++) {
				FSDataInputStream in = fs.open(new Path(inputFiles[k]));
				
				String data;
				double weight;
				
				long firstDate;
				long secondDate;
				
				GregorianCalendar calendar = new GregorianCalendar();
				calendar.setTime(new Date());
				calendar.add(Calendar.MONTH, -11);
				firstDate = calendar.getTime().getTime();
				
				calendar = new GregorianCalendar();
				calendar.setTime(new Date());
				calendar.add(Calendar.MONTH, -3);
				secondDate = calendar.getTime().getTime();
				
				String[] names = readLine(in).split("\\|");
				
				readLine(in);
				
				while((data = readLine(in)) != null) {
					String[] gpathData = data.split("\\|");
					if(gpathData.length < 2) break;
					
					String result = "";
					
					if(reverce[k])
						result = columnNames[k][1] + gpathData[1].trim() + " " + columnNames[k][0] + gpathData[0].trim() + " ";
					else
						result = columnNames[k][0] + gpathData[0].trim() + " " + columnNames[k][1] + gpathData[1].trim() + " ";
					
					weight = 1;
					
					if(names.length == 3) {
						DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
						
						try {
							float date = df.parse(gpathData[2]).getTime();
							if(date < firstDate)
								weight = 0;
							else if(date > secondDate)
								weight = 1;
							else
								weight = (date - firstDate) / (secondDate - firstDate);
						} catch (ParseException e) {}
					}
					result += (new Float(weight)).toString();
					result += "\n";
					
					out.write(result.getBytes());
				}
							
				in.close();
			}
			
			out.close();
		} catch (IOException ioe) {
			System.err.println("IOException during operation: " + ioe.toString());
			System.exit(1);
		}
	}
}