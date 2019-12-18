# Alibaba Graph Database Driver for Java
GDB Java语言客户端，参考Gremlin Java开源客户端[gremlin-driver](https://github.com/apache/tinkerpop/tree/master/gremlin-driver). 
其与官方Driver不同点在于：
- 目前支持sessionLess-script、sessionLess-bytecode、session-script、session-bytecode四种几乎方式
- 如果使用alibaba gdb服务，可以享受GraphBinaryMessageSerializerV1协议交互时，获取detachedElement
- 如果使用alibaba gdb服务，可以获取数据导入扩展能力
## 安装
```cfml
下载
git clone https://github.com/aliyun/alibabacloud-gdb-java-sdk

编译
mvn clean install
```

## 使用范例
```c
SDK参考范例：
alibabacloud-gdb-java-sdk/src/main/java/com/alibaba/gdb/java/client/Demo.java

GDB服务配置：
alibabacloud-gdb-java-sdk/src/main/resources/gdb-remote.yaml
```
