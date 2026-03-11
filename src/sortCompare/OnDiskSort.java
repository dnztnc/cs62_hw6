package sortCompare;

import java.io.*;
import java.util.*;

/**
 * Implements an external (on-disk) mergesort to efficiently sort data that do
 * not fit into the main memory.
 * Instead, it reads files from the hard disk drive, loads in main memory small
 * chunks that fit in it, sorts them individually, and saves them in temporary
 * files.
 * It then repeatedly merges these temporary files into increasingly larger
 * sorted files until it sorts the entire original dataset.
 */
public class OnDiskSort {

	// TODO: add instance variables

	/**
	 * Creates a new sorter for sorting string data on disk. The sorter operates by
	 * reading in maxSize worth of data elements (in this case, Strings) and then
	 * sorts them using the provided sorter. It does this chunk by chunk for all of
	 * the data, at each stage writing the sorted data to temporary files in
	 * workingDirectory. Finally, the sorted files are merged together (in pairs)
	 * until there is a single sorted file. The final output of this sorting should
	 * be in outputFile
	 *
	 * @param maxSize
	 *                         the maximum number of items to put in a chunk
	 * @param workingDirectory
	 *                         the directory where any temporary files created
	 *                         during sorting
	 *                         should be placed
	 * @param sorter
	 *                         the sorter to use to sort the chunks in memory
	 */
	public OnDiskSort(int maxSize, File workingDirectory, Sorter<String> sorter) {
		// TODO: update instance variables

		// create directory if it doesn't exist
		if (!workingDirectory.exists()) {
			workingDirectory.mkdir();
		}
	}

	/**
	 * Remove all files that end with fileEnding from the workingDirectory
	 *
	 * If you name all of your temporary files with the same file ending, for
	 * example ".temp_sorted"
	 * then it's easy to clean them up using this method
	 *
	 * @param workingDirectory the directory to clear
	 * @param fileEnding       clear only those files with fileEnding
	 */
	private void clearOutDirectory(File workingDirectory, String fileEnding) {
		for (File file : workingDirectory.listFiles()) {
			if (file.getName().endsWith(fileEnding)) {
				file.delete();
			}
		}
	}

	/**
	 * Write the Strings stored in dataToWrite to outfile one String per line
	 *
	 * @param outfile     the output file
	 * @param dataToWrite the String data to write out
	 */
	private void writeToDisk(File outfile, ArrayList<String> dataToWrite) {
		try {
			PrintWriter out = new PrintWriter(new FileOutputStream(outfile));

			for (String s : dataToWrite) {
				out.println(s);
			}

			out.close();
		} catch (IOException e) {
			throw new RuntimeException(e.toString());
		}
	}

	/**
	 * Copy data from fromFile to toFile
	 *
	 * @param fromFile the file to be copied from
	 * @param toFile   the destination file to be copied to
	 */
	private void copyFile(File fromFile, File toFile) {
		try {
			BufferedReader in = new BufferedReader(new FileReader(fromFile));
			PrintWriter out = new PrintWriter(new FileOutputStream(toFile));

			String line = in.readLine();

			while (line != null) {
				out.println(line);
				line = in.readLine();
			}

			out.close();
			in.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Sort the data in dataReader using an on-disk version of sorting
	 *
	 * @param dataReader
	 *                   an Iterator that allows us to "scan"/read the data to be
	 *                   sorted
	 * @param outputFile
	 *                   the destination for the final sorted data
	 */
	public void sort(WordScanner dataReader, File outputFile) {
		// TODO: read one String at a time fro the dataReader and save it in an
		// ArrayList of maximum capacity maxSize.
		// When your ArrayList reaches maxSize elements, call the sort method of the
		// sorter to sort the chunk, write
		// to disk the temporary file that holds maxSize sorted Strings, and add that
		// file in an ArrayList of sorted files.
		// Once this process is completed, call mergeFiles passing the ArrayList of
		// sorted files which will be responsible
		// in repeatedly merging them an increasingly larger sorted file which will be
		// eventually sorted in outputFile.
		// Don't forget to create out the working directory when you are done.
	}

	/**
	 * Merges all the Files in sortedFiles into one sorted file, whose destination
	 * is outputFile.
	 *
	 * @pre All of the files in sortedFiles contained data that is sorted
	 * @param sortedFiles a list of files containing sorted data
	 * @param outputFile  the destination file for the final sorted data
	 */
	protected void mergeFiles(ArrayList<File> sortedFiles, File outputFile) {
		// TODO: the easiest way to do this is to have a temporary file that contains
		// all of
		// your merged data so far and then just merge in one more file.
		// To start with, we can just copy the first sortedFile to the temp file and
		// then
		// merge in the remaining files linearly.
		// Look into the copyFile and merge methods and don't forget edge cases.
	}

	/**
	 * Given two files containing sorted strings, one string per line, merge them
	 * into one sorted file
	 *
	 * @param file1   file containing sorted strings, one per line
	 * @param file2   file containing sorted strings, one per line
	 * @param outFile destination file for the results of merging the two files
	 */
	protected void merge(File file1, File file2, File outFile) {
		// TODO: Open two BufferedReaders to read file1 and file2.
		// Compare the first line of file1 with the first line of file2.
		// If it is smaller, add it to the outFile and proceed to the second line of
		// file1.
		// If it is larger, add the first line of file2 to the outFile and proceed to
		// the second line of file2.
		// If you run out of data from one file, you know that the data from the second
		// contain only "larger" Strings.
		// You can just append them to the outFile.
	}

	/**
	 * Create a sorter that does a mergesort in memory
	 * Create a diskSorter to do external merges
	 * Use subdirectory "sorting_run" of your project as the working directory
	 * Create a word scanner to read King's "I have a dream" speech.
	 * Sort all the words of the speech and put them in file data.sorted
	 *
	 * @param args -- not used!
	 */
	public static void main(String[] args) {
		MergeSort<String> sorter = new MergeSort<String>();
		OnDiskSort diskSorter = new OnDiskSort(10, new File("sorting_run"), sorter);

		WordScanner scanner = new WordScanner(new File("sorting_run//Ihaveadream.txt"));

		System.out.println("running");
		diskSorter.sort(scanner, new File("sorting_run//data.sorted"));
		System.out.println("done");
	}

}
