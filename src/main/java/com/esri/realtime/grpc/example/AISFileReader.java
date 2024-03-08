package com.esri.realtime.grpc.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

/**
 * A helper class that provides functions of reading the AIS data file
 * and returning data as AISData objects or String.
 */
public class AISFileReader {
  String         filePath = "ais1.csv";
  BufferedReader reader;

  public AISFileReader() {
    try {
      InputStream inputStream = AISFileReader.class.getClassLoader().getResourceAsStream(filePath);
      reader = new BufferedReader(new InputStreamReader(inputStream));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Reads a number of records from the file and return an array of AISData objects
   *
   * @param count Records to read
   * @return an array of AISData objects
   * @throws IOException
   */
  public AISData[] getData(int count) throws IOException {
    String line;
    int remaining = count;
    ArrayList<AISData> shipList = new ArrayList<>();
    while ((line = reader.readLine()) != null && remaining > 0) {
      // Process each line as needed
      System.out.println(line);
      AISData ship = AISData.parseData(line);
      if (ship != null) {
        shipList.add(ship);
        remaining --;
      }
    }
    return shipList.toArray(new AISData[0]);
  }

  /**
   * Reads a number of records from the file and return a string.  Each line of the string
   * is a comma-delimited record of AIS data.
   *
   * @param count Records to read
   * @return a string that consists of comma-delimited AIS data records
   * @throws IOException
   */
  public String getDelimitedData(int count) throws IOException {
    String line;
    int remaining = count;
    ArrayList<String> shipList = new ArrayList<>();
    while ((line = reader.readLine()) != null && remaining > 0) {
      // Process each line as needed
      System.out.println(line);
      shipList.add(line);
      remaining --;
    }
    return String.join("\n", shipList.toArray(new String[0]));
  }

  /**
   * closes the reader
   */
  public void close() {
    if (reader != null) {
      try {
        reader.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
