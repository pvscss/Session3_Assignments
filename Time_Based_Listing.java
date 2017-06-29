import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.*;

public class Time_Based_Listing 
{

	public static void main(String[] args) throws Exception 
	{
		
		Configuration conf=new Configuration();
		conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
		
		FileSystem fs=FileSystem.get(conf);
		
		Scanner scanner=new Scanner(System.in);
		
		System.out.println("Display List of Directories based on Latest Modified TimeStamp");
		ListbyTimeStamp(fs,scanner);
		
		System.out.println("Display the Content of a File");
		ReadContentofFile(fs,scanner);
		
		System.out.println("Copy a File from Local File System to HDFS File System");
		CopyFromLocalToHDFS(fs,scanner);
		
		fs.close();
		
		scanner.close();

	}
	
	private static void ListbyTimeStamp(FileSystem fs,Scanner scanner) throws Exception
	{
		
		System.out.println("Initialising Start and End Time");
		
		long start_ts=0;
		long end_ts=Long.MAX_VALUE;
		
		System.out.println("Enter the Directory to show the List of Files and Directories :");
		String DirPath=scanner.next();
		
		Path path=new Path(DirPath);
		
		System.out.println("Enter the start Time Stamp Range");
		start_ts=scanner.nextLong();
		System.out.println("Enter the end Time Stamp Range");
		end_ts=scanner.nextLong();
		
		FileStatus[] fs1=fs.listStatus(path);
		
		for(FileStatus fs2:fs1)
		{
			long Latest_TS=fs2.getModificationTime();
			if(Latest_TS>=start_ts && Latest_TS<=end_ts)
			{
				if (fs2.isDirectory()) {
					System.out.println("Directory: " + fs2.getPath());
					System.out.println("TimeStamp Value"+Latest_TS);
				}
				else if (fs2.isFile()) {
					System.out.println("File: " + fs2.getPath());
					System.out.println("TimeStamp Value"+Latest_TS);
				}
				else if (fs2.isSymlink()) {
					System.out.println("Symlink: " + fs2.getPath());
					System.out.println("TimeStamp Value"+Latest_TS);
				}
			}
			
		}
		
	}
	
	private static void ReadContentofFile(FileSystem fs,Scanner scanner) throws Exception
	{
		System.out.println("Enter the File to Read :");
		String File_Path=scanner.next();
		
		Path path=new Path(File_Path);
		
		FSDataInputStream ip_Stream=fs.open(path);
		System.out.println("File Opened :"+File_Path);
		
		System.out.println("Contents will be displayed Below :");
		BufferedReader br=new BufferedReader(new InputStreamReader(ip_Stream));
		String line=br.readLine();
		
		while(line!=null)
		{
			System.out.println("Line Read="+line);
			line=br.readLine();
		}
		br.close();
		ip_Stream.close();
	}
	
	private static void CopyFromLocalToHDFS(FileSystem fs,Scanner scanner) throws Exception
	{
		System.out.println("Enter the File Path from where to be Copied :");
		String File_Path=scanner.next();
		
		Path path=new Path(File_Path);
		
		System.out.println("Enter the Destination Path :");
		String Dest_Path=scanner.next();
		
		Path path1=new Path(Dest_Path);
		
		FileStatus[] fs1=fs.listStatus(path1);
		
		for(FileStatus fs2:fs1)
		{
			System.out.println(fs2.getPath());
		}
		
		fs.copyFromLocalFile(path, path1);
		
		for(FileStatus fs2:fs1)
		{
			System.out.println(fs2.getPath());
		}
	}
}
