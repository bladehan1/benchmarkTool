package benchmarktool;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BinaryFileWriter {
  // 魔数标识文件类型（"LVDB" 的 ASCII 码）
  static final int MAGIC_NUMBER = 0x4C564442;
  static final short VERSION = 0x0001;

  public static void writeToFile(String filename,  List<byte[]> keyList) throws IOException {
    try (DataOutputStream dos = new DataOutputStream(
        new BufferedOutputStream(new FileOutputStream(filename)))) {
      // 写入头部信息
      dos.writeInt(MAGIC_NUMBER);
      dos.writeShort(VERSION);
      dos.writeInt(keyList.size());
      // 写入键值对数据
      for (byte[] key : keyList) {
        // 写入 Key
        dos.writeShort(key.length);
        dos.write(key);
      }
    }
  }

  public static void main(String[] args) throws IOException {
    // 示例数据
    List<byte[]>  data = new ArrayList<>();
    data.add("key1".getBytes());
    data.add("key2".getBytes());
    // 写入二进制文件
    writeToFile("data.bin",data);
    System.out.println("数据写入完成");
  }
}