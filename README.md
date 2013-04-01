twister简介
=============================================================================
(1)使用storm框架流式实时统计分析
(2)支持Nio的tcp/udp(netty)
(3)cache部分ehcache map
(4)对象化AccessLog

Twitter Storm本机模式下安装部署(debug)
=============================================================================
下载Storm，地址为https://github.com/nathanmarz/storm，Storm用于将JAR包和Topology的主类提交给nimbus。
本地模式我们只需记住一个命令：storm jar storm-starter-0.0.1-SNAPSHOT-jar-with-dependencies storm.starter.WordCountTopology
下载strom-starter,地址为https://github.com/nathanmarz/storm-starter，在本地模式跑通里面的WordCountTopology例子 并将bin目录下的storm脚本设置成可执行模式。
执行完上步骤后，会在storm-starter文件夹下生成一个target目录，里面生成两个Jar包。一个是storm-starter-0.0.1-SNAPSHOT.jar，
另一个是 storm-starter-0.0.1-SNAPSHOT-jar-with-dependencies.jar，
然后在此目录运行storm jar storm-starter-0.0.1-SNAPSHOT-jar-with-dependencies.jar storm.starter.WordCountTopology 这个命令就会在本机模式下，
用线程模拟storm平台执行topology。执行结果显示无误的话，输出会有10000多行。 

Twitter Storm 的远程模式安装部署(0.8.2)  
=============================================================================
一．Storm平台的组件包括： Storm client，nimbus节点，zookeeper集群（我们暂时部署一个节点），和worker节点（我们部署三个节点）。
二．Storm 客户端
   Storm负责将Topology代码提交给Nimbus。
   客户端所装软件包括
   1）storm源码，托管在github上。下载地址https://github.com/nathanmarz/storm/downloads。
    下载后，将/bin加到PATH变量。并将/bin目录下的storm的权限改为可执行。
   2）需要下载Maven。它可以进行源代码编译并对pom.xml（一个Storm程序例子的依赖关系写这里）中的依赖包进行下载管理。用法和本地模式一样。
   3)JDK 1.7版本。设置好PATH和CLASSPATH。
三．Nimbus
Nimbus是master节点。负责接收Storm客户端提交的代码，并将代码分发给worker节点。
   所需软件包括：
   1)Storm源码，安装步骤和Storm客户端相同
   2）ZeroMQ,负责消息队列。
   下载地址http://www.zeromq.org/area:download。版本号为2.2，下载到/opt/software/。
   利用tar –zxvf 解压。cd解压后的目录。./configure->make->make install.
   3)下载JZMQ,作为ZeroMQ的java语言绑定。
   下载地址https://github.com/nathanmarz/jzmq。 
   tar -zxvf 解压。cd 解压后的目录。
   ./autogen.sh->./configure->make->make install。在./autogen.sh的时候会出现错误。
    原因就是没有libtool。因为在部署的时候只有10.141.9.227这台redhat主机没有libtool，autoconf，automake工具。所以从安装盘里拷贝出rpm进行安装。
  安装命令为  rpm –Uvh ****.rpm。开始阶段出现各种错误，主要是没有找对对应的libtool版本。经验就是一定要找对这种依赖库或工具的版本。
  4）安装python2.7.3。
   安装完后运行指令python,会出现python控制台，查看是否为python2.7.3版本，如果不是进入/usr/bin/下
   ，将python的链接更改为链接到python2.7.3。ln –s  python python2.7.3
   当然上述命令的python2.7.3处应该写python2.7.3的路径。
   5）下载JDK1.7以上版本。
   6）安装unzip。
四．所有的Worker节点
   所装软件和Nimbus节点一样。
五．Zookeeper集群 。
   1）安装JDK 1.7以上。
   2）解压zookeeper。下载地址：http://zookeeper.apache.org/doc/r3.3.3/zookeeperAdmin.html。
   进入解压目录，将入conf目录下的配置文件更改为zoo.cfg。
   然后对zoo.cfg进行配置。Zookeeper的部署需要奇数台机器，这是由于当Zookerper的机器down掉了一半之后，如果是偶数台机器，集群就会崩溃。
   这和Zookeeper的同步机制有关。
六．配置storm.yaml。
   在storm目录下的conf文件夹内。这里出现的问题就是没有把配置文件内的井号去掉，以至于配置文件根本没有起作用，实在不应该啊。
   在地址https://github.com/nathanmarz/storm/blob/master/conf/defaults.yaml。是storm.yaml的默认配置。刚开始测试时必须改一下几条：
   1  #storm.local.dir: "/mnt/storm"，改成适合自己平台的地址。这文件夹存储storm的本地文件。
   2  #storm.cluster.mode: "distributed" ，采取的远程模式，而不是本地模式。
   3  #nimbus.host:”10.141.9.227”,nimbus的地址。
主要就是以上几项：其他可以使用默认配置。 
---------------------------------------------------------------------
当软件都安装好之后，开始启动相应的进程。
启动 nimbus   :  storm nimbus
启动 worker   :  storm supervisor
启动 ui       :  strom ui
启动 zookeeper:  .zkserver start
对storm启动的进程，会一直处于命令的运行的状态中，不能按CRTL + C。可以再开一个终端。而且，storm 启动的进程，在不需要的时候要kill掉。否则会堆积很多进程。
在浏览器中输入http://nimbus_ip:8080来查看storm 平台的运行信息。
Storm ui 已经正确的显示出来，还没有跑例子。下面的事情就是跑下例子。然后在看源码学习吧。

Twitter Storm 参考资料
=============================================================================
参考：http://blog.csdn.net/larrylgq/article/details/7237832
    http://xumingming.sinaapp.com/179/twitter-storm-搭建storm集群/
 
twister运行   
=============================================================================
cd workspace
git clone https://github.com/zhouguoqing917/twister.git twister
mvn -Dmaven.test.skip=true clean package
1)debug 环境
storm jar ./target/twister-0.0.1-jar-with-dependencies.jar com.twister.topology.WordCountTopology wordCountTopology
2)启动 topology
storm jar ./target/twister-0.0.1-jar-with-dependencies.jar com.twister.topology.TwisterTopology
3)启动 niotcp send log
java -cp classes -classpath ./target/twister-0.0.1-jar-with-dependencies.jar:. com.twister.nio.SendNioTcpClient  <host> <port> [<accessFile>]

 