# Alibaba Graph Database Driver for Java
GDB Java语言客户端，参考Gremlin Java开源客户端[gremlin-driver](https://github.com/apache/tinkerpop/tree/master/gremlin-driver). 
其与官方Driver不同点在于：
- 目前支持sessionLess-script、sessionLess-bytecode、session-script、session-bytecode四种交互方式
- 如果使用alibaba gdb服务，可以享受GraphBinaryMessageSerializerV1协议交互时，获取detachedElement
- 如果使用alibaba gdb服务，可以获取数据导入扩展能力
- 高可用接入服务，写请求、session类请求访问主节点，其他只读请求优先访问只读节点，自动失败重试
  
## 安装
```cfml
下载
git clone https://github.com/aliyun/alibabacloud-gdb-java-sdk

编译
mvn clean install
```

## 高可用接入
### 配置方式
```cfml
gdb-remote.yaml中增加以下项目:
    readonlyHostsHA: {
      # true:  read request first route to readonly hosts(retry if host fail), 
      #         write request route to master directly;
      # false: both read and write request will route to master
      state: true,

      # readonly hosts endpoints
      readonlyHosts: [gdr-read1.graphdb.rds.aliyuncs.com, gdr-read2.graphdb.rds.aliyuncs.com],


      # request's max retry time before client's failure
      retryCnt: 1,

      # request default timeout to read only hosts(ms)
      # -1: the same as master, > 0: use defaultTimeOut
      defaultTimeout: 5000,

      # timeout policy, 0: kick out, 1: no-action
      timeoutPolicy: 0
    }
```
- 如果关闭只读实例高可用能力，可以将state设置为false
- 如果有多个只读实例，可以合理设置retryCnt，这里需要注意：多次retry的超时累积为业务请求的最大超时
- 如果希望修改只读请求的单次超时时间, 可以设置defaultTimeout(否则其值和master超时时间相同，默认为30s)
- 如果希望超时立即剔除只读节点，可以将timeoutPolicy设置为0（后续探活机制会再次将其加入集群）
- 只读失败分为两类：超时类，其他类，两种都会重试，并不影响用户最终结果

### 使用限制
目前支持：sessionLess-script、sessionLess-bytecode、session-script、session-bytecode四种方式，
不同方式高可用支持能力如下：
- sessionLess-script: 支持
- sessionLess-bytecode: 支持，bytecode的执行或者翻译执行再server端进行
- session-script: 不支持(所有请求访问master节点)
- session-bytecode: 不支持(所有请求访问master节点), bytecode再client端翻译，server执行script sql

## 使用范例
```cfml
SDK参考范例：
- 四种高可用接入：alibabacloud-gdb-java-sdk/src/main/java/test/client/ClusterReadModeTest.java
- 四种交互方式：alibabacloud-gdb-java-sdk/src/main/java/test/client/InteractionMethodTest.java
- 导入交互：alibabacloud-gdb-java-sdk/src/test/java/test/loader/GDBLoaderDemo.java

GDB服务配置：
- alibabacloud-gdb-java-sdk/config/gdb-remote.yaml
```
