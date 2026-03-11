package sortCompare;

import java.util.Scanner;
import java.util.regex.Pattern;
import java.util.NoSuchElementException;
import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.util.Iterator;

/**
 * WordScanner
 *
 * Reads from the input stream word by word
 */
public class WordScanner implements Iterator<String> {
	private Scanner theScanner;
	private static final Pattern wordPattern = Pattern.compile("[a-zA-Z]*");

	private static final Pattern separatorPattern = Pattern.compile("[^a-zA-Z]*");

	public WordScanner(File file) {
		try {
			theScanner = new Scanner(file);
		} catch (IOException e) {
			assert false : "File not found";
		}
	}

	public WordScanner(InputStream str) {
		theScanner = new Scanner(str);
	}

	public WordScanner(String name) {
		theScanner = new Scanner(name);
	}

	public WordScanner(Readable src) {
		theScanner = new Scanner(src);
	}

	protected void skipToNextWord() {
		try {
			theScanner.skip(separatorPattern);
		} catch (NoSuchElementException e) {
			// do nothing
		}
	}

	public String next() {
		skipToNextWord();
		return theScanner.findInLine(wordPattern);
	}

	public boolean hasNext() {
		try {
			skipToNextWord();
			return theScanner.hasNext();
		} catch (IllegalStateException e) {
			return false;
		}
	}

	public void close() {
		theScanner.close();
	}

	public void remove() {
		// not a required method, so we won't do anything
	}
}
