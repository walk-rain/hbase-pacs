# 影像图片存储服务调用说明

Socket服务地址：139.9.115.81

Socket服务端口：2323

## 1 信息格式说明

### 1.1 控制信息

格式：`类型`^`文件名(全局唯一，不能重复)`^`文件长度`^`患者登记号`

| 序号 | 名称       | 说明                     | 是否必填                     |
| ---- | ---------- | ------------------------ | ---------------------------- |
| 1    | 类型       | w: 上传文件，r: 读取文件 | 是                           |
| 2    | 文件名     | 全局唯一，不能重复       | 是                           |
| 3    | 文件长度   | 字节数                   | 类型为w时为是，类型为r时为否 |
| 4    | 患者登记号 |                          | 否                           |

### 1.2 文件信息

字节流

## 2 调用过程

先传输控制信息，再上传文件或下载文件

### 2.1 上传文件

调用示例：

```java
File file = new File("D:\\Test\\input\\195251-1608033171c818.jpg");
long fileLength = file.length();

try(Socket socket = new Socket("139.9.115.81", 2323)) {

    OutputStream outputStream = socket.getOutputStream();
    //使用DataOutputStream
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
    //向服务器端传输控制信息
    dataOutputStream.writeUTF("w^" + file.getName() + "^" + fileLength);
    dataOutputStream.flush();//刷新流，传输到服务端

    //向服务器端传文件，通过字节流
    //字节流先读取硬盘文件
    BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(file));

    byte[] bytes = new byte[1024];
    int len = 0;
    while ((len=bufferedInputStream.read(bytes))!=-1) {
        dataOutputStream.write(bytes,0,len);
        dataOutputStream.flush();
    }

} catch (IOException e) {
    e.printStackTrace();
}
```

### 2.2 下载文件

调用示例：

```java
String fileName = "195251-1608033171c818.jpg";
try(Socket socket = new Socket("139.9.115.81", 2323)) {

    OutputStream outputStream = socket.getOutputStream();
    //使用DataOutputStream
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
    //向服务器端传输控制细腻
    dataOutputStream.writeUTF("r^" + fileName);
    dataOutputStream.flush();//刷新流，传输到服务端

    DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
    FileOutputStream fileOutputStream = new FileOutputStream("D:\\Test\\output" + File.separator + fileName);
    byte[] bytes = new byte[1024];
    int len = 0;
    while ((len=dataInputStream.read(bytes))!=-1) {
        fileOutputStream.write(bytes,0,len);
        fileOutputStream.flush();
    }
    fileOutputStream.close();
} catch (IOException e) {
    e.printStackTrace();
}
```

