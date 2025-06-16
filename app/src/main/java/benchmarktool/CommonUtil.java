package benchmarktool;

public class CommonUtil {
  public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  // 字节数组转十六进制字符串
  public static String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }

  // 十六进制字符串转字节数组
  private static byte[] hexToBytes(String hex) {
    int len = hex.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
          + Character.digit(hex.charAt(i+1), 16));
    }
    return data;
  }

  public static byte[] fromHexString(String data) {
    if (data == null) {
      return EMPTY_BYTE_ARRAY;
    }
    if (data.startsWith("0x")) {
      data = data.substring(2);
    }
    if (data.length() % 2 != 0) {
      data = "0" + data;
    }
    return hexToBytes(data);
  }

  // 使用示例
//  byte[] data = {0x12, 0x34, 0x5F};
//  String hex = bytesToHex(data); // "12345f"
//  byte[] decoded = hexToBytes("A0B1"); // {0xA0, 0xB1}
}
