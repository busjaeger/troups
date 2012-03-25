package edu.illinois.htx.tm;

public interface HTXConstants {

  // internal constants

  public static final String ATTR_NAME_TTS = "htx-tts";

  public static final String ATTR_NAME_DEL = "htx-del";

  // configuration properties

  public static final String TSO_PORT = "htx.tm.port";

  public static final int DEFAULT_TSO_PORT = 60001;

  public static final String TSO_HANDLER_COUNT = "htx.tm.handler.count";

  public static final int DEFAULT_TSO_HANDLER_COUNT = 25;

}