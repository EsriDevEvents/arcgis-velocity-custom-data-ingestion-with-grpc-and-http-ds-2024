package com.esri.realtime.grpc.example;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Timer;
import java.util.TimerTask;

public class AISExampleHttpPost {

  // Connection parameters
  public static final String HTTP_RECEIVER_ENDPOINT = "https://yourserver.com/aaa/receiver/bbb";

  // Interval, volume and duration information related to sending data
  public static final int SEND_INTERVAL_MS = 1000;
  public static final int PER_SEND_SIZE = 200;
  public static final int MAX_RUN_TIME_MS = 10000;

  public static void main(String[] args) throws InterruptedException {
    AISFileReader reader = new AISFileReader();

    try {
      URL url = new URL(HTTP_RECEIVER_ENDPOINT);

      Timer timer = new Timer();
      long startTime = System.currentTimeMillis();
      // Schedule a new TimerTask
      timer.scheduleAtFixedRate(new TimerTask() {
        @Override
        public void run() {
          try {
            // Open a connection to the URL
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();

            // Set the request method to POST
            connection.setRequestMethod("POST");

            // Enable input/output streams and set other properties as needed
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "text/plain");

            // Send a string that contains an array of AIS data records
            try (OutputStream outputStream = connection.getOutputStream()) {
              String postData = reader.getDelimitedData(PER_SEND_SIZE);
              byte[] input = postData.getBytes("utf-8");
              outputStream.write(input, 0, input.length);
            } catch (Exception e) {
              e.printStackTrace();
            }

            // Get the response code
            int responseCode = connection.getResponseCode();
            System.out.println("Response Code: " + responseCode);
            connection.disconnect();

            // Check the time it has been running.  Close down if exceeds MAX_RUN_TIME_MS. 
            if (System.currentTimeMillis() - startTime >= MAX_RUN_TIME_MS) {
              if (reader != null)
                reader.close();

              System.out.println("Done.");
              timer.cancel();
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }, 100, SEND_INTERVAL_MS);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
