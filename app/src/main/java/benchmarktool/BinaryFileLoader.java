package benchmarktool;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BinaryFileLoader {
  public static List<byte[]> loadFromFile(String filename) throws IOException, InvalidDataException {
    List<byte[]> data=new ArrayList<>();
    try (DataInputStream dis = new DataInputStream(
        new BufferedInputStream(new FileInputStream(filename)))) {
      // 读取头部信息
      int magic = dis.readInt();
      if (magic != BinaryFileWriter.MAGIC_NUMBER) {
        throw new InvalidDataException("无效的文件格式");
      }

      short version = dis.readShort();
      if (version != BinaryFileWriter.VERSION) {
        throw new InvalidDataException("不兼容的版本号");
      }

      int entryCount = dis.readInt();

      // 读取键值对数据
      for (int i = 0; i < entryCount; i++) {
        // 读取 Key
        int keyLength = dis.readUnsignedShort();
        byte[] key = new byte[keyLength];
        dis.readFully(key);
        data.add( key);
      }
    }
    System.out.println("data size: "+data.size());
    return data;
  }

  public static void main(String[] args) {
    try {
      List<byte[]> loadedData = loadFromFile("data.bin");
      System.out.println("数据加载成功，条目数: " + loadedData.size());
      // 打印示例数据
      loadedData.forEach((key) ->
          System.out.println("Key: " + new String(key) ));
    } catch (IOException | InvalidDataException e) {
      System.err.println("加载失败: " + e.getMessage());
    }
  }
}

// 自定义异常类
class InvalidDataException extends Exception {
  public InvalidDataException(String message) {
    super(message);
  }
}