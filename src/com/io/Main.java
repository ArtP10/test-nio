package com.io;
import java.io.IOException;

import files.CreateDirectory;
import files.CreateFile;
import files.CustomFileReader;
import files.NewFileReader;


public class Main {

	public static void main(String[] args) throws IOException, InterruptedException {
		System.out.println("Code Started");
		//NewFileReader newFileReader = new NewFileReader();
		//newFileReader.readLargeFile();

		CustomFileReader processor = new CustomFileReader();
        long startTime = System.nanoTime();
        try {
            processor.processFolder();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        long endTime = System.nanoTime();
        long duration = (endTime - startTime) / 1_000_000; // milliseconds
        System.out.println("Total folder processing and writing time: " + duration + " ms");
	
		
	}

}
