package com.esri.realtime.grpc.example;

import com.esri.realtime.core.grpc.Feature;
import com.google.protobuf.Any;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.StringValue;

public class AISData {

  protected Integer mmsi;           // TRACK_ID
  protected String baseDateTime;    // START_TIME
  protected Double lat;
  protected Double lon;
  protected Double sog;
  protected Double cog;
  protected Double heading;
  protected String vesselName;
  protected String imo;
  protected String callSign;
  protected Integer vesselType;
  protected Integer status;
  protected Integer length;
  protected Integer width;
  protected Double draft;
  protected Integer cargo;
  protected String transceiverClass;

  /**
   * This method returns an AISData object with sample values
   *
   * @return an AISData object
   */
  public static AISData getOne() {
    AISData ship = new AISData();
    ship.mmsi = 367077850;
    ship.baseDateTime = "2023-02-02T00:17:30";
    ship.lat = 28.32581;
    ship.lon = -96.22587;
    ship.sog = 6.0;
    ship.cog = 141.3;
    ship.heading = 131.0;
    ship.vesselName = "CHIPOLBROK ATLANTIC";
    ship.imo = "IMO9731377";
    ship.callSign = "VRPP6";
    ship.vesselType = 70;
    ship.status = 0;
    ship.length = 189;
    ship.width = 25;
    ship.draft = 4.4;
    ship.cargo = 79;
    ship.transceiverClass = "A";

    return ship;
  }

  /**
   *
   * @param line An AIS record in the form of a comma-delimited string
   *
   * @return an AISData object
   */
  public static  AISData parseData(String line) {
    String[] attributes = line.split(",");
    AISData ship = new AISData();
    try {
      ship.mmsi = attributes[0].isEmpty()?null:Integer.parseInt(attributes[0]);
      ship.baseDateTime = attributes[1];
      ship.lat = attributes[2].isEmpty()?null:Double.parseDouble(attributes[2]);
      ship.lon = attributes[3].isEmpty()?null:Double.parseDouble(attributes[3]);
      ship.sog = attributes[4].isEmpty()?null:Double.parseDouble(attributes[4]);
      ship.cog = attributes[5].isEmpty()?null:Double.parseDouble(attributes[5]);
      ship.heading = attributes[6].isEmpty()?null:Double.parseDouble(attributes[6]);
      ship.vesselName = attributes[7];
      ship.imo = attributes[8];
      ship.callSign = attributes[9];
      ship.vesselType = attributes[10].isEmpty()?null:Integer.parseInt(attributes[10]);
      ship.status = attributes[11].isEmpty()?null:Integer.parseInt(attributes[11]);
      ship.length = attributes[12].isEmpty()?null:Integer.parseInt(attributes[12]);
      ship.width = attributes[13].isEmpty()?null:Integer.parseInt(attributes[13]);
      ship.draft = attributes[14].isEmpty()?null:Double.parseDouble(attributes[14]);
      ship.cargo = attributes[15].isEmpty()?null:Integer.parseInt(attributes[15]);
      ship.transceiverClass = attributes[16];
    } catch (Exception e) {
      System.out.println("Problem parsing line: " + line);
      ship = null;
    }
    return ship;
  }

  /**
   * Converts an AISData object into a protobuf feature which is defined in the proto file.
   * Since the gRPC client sends or streams data in binary format, the order of the attributes
   * and their data types must match the feature schema that is used to create the feed.
   *
   * @param ship an AISData object
   * @return Feature
   */
  public static Feature asFeature(AISData ship) {
    Feature.Builder featureBuilder = Feature.newBuilder();
    featureBuilder.addAttributes(Any.pack(Int32Value.of(ship.mmsi)));
    featureBuilder.addAttributes(Any.pack(StringValue.of(ship.baseDateTime)));
    if (ship.lat == null)
      featureBuilder.addAttributes(Any.getDefaultInstance());
    else
      featureBuilder.addAttributes(Any.pack(DoubleValue.of(ship.lat)));
    if (ship.lon == null)
      featureBuilder.addAttributes(Any.getDefaultInstance());
    else
      featureBuilder.addAttributes(Any.pack(DoubleValue.of(ship.lon)));
    if (ship.sog == null)
      featureBuilder.addAttributes(Any.getDefaultInstance());
    else
      featureBuilder.addAttributes(Any.pack(DoubleValue.of(ship.sog)));
    if (ship.cog == null)
      featureBuilder.addAttributes(Any.getDefaultInstance());
    else
      featureBuilder.addAttributes(Any.pack(DoubleValue.of(ship.cog)));
    if (ship.heading == null)
      featureBuilder.addAttributes(Any.getDefaultInstance());
    else
      featureBuilder.addAttributes(Any.pack(DoubleValue.of(ship.heading)));
    featureBuilder.addAttributes(Any.pack(StringValue.of(ship.vesselName)));
    featureBuilder.addAttributes(Any.pack(StringValue.of(ship.imo)));
    featureBuilder.addAttributes(Any.pack(StringValue.of(ship.callSign)));
    if (ship.vesselType == null)
      featureBuilder.addAttributes(Any.getDefaultInstance());
    else
      featureBuilder.addAttributes(Any.pack(Int32Value.of(ship.vesselType)));
    if (ship.status == null)
      featureBuilder.addAttributes(Any.getDefaultInstance());
    else
      featureBuilder.addAttributes(Any.pack(Int32Value.of(ship.status)));
    if (ship.length == null)
      featureBuilder.addAttributes(Any.getDefaultInstance());
    else
      featureBuilder.addAttributes(Any.pack(Int32Value.of(ship.length)));
    if (ship.width == null)
      featureBuilder.addAttributes(Any.getDefaultInstance());
    else
      featureBuilder.addAttributes(Any.pack(Int32Value.of(ship.width)));
    if (ship.draft == null)
      featureBuilder.addAttributes(Any.getDefaultInstance());
    else
      featureBuilder.addAttributes(Any.pack(DoubleValue.of(ship.draft)));
    if (ship.cargo == null)
      featureBuilder.addAttributes(Any.getDefaultInstance());
    else
      featureBuilder.addAttributes(Any.pack(Int32Value.of(ship.cargo)));
    featureBuilder.addAttributes(Any.pack(StringValue.of(ship.transceiverClass)));

    return featureBuilder.build();
  }
}
