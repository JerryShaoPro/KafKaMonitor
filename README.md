# KafKaMonitor 说明
kafka监控工具，监控logsize,offset，适用于kafka0.7版本。【修改自网络上的0.8.x的版本，完成kafka0.7.x和scala2.8版本的兼容】


## 开发、打包工程
构建eclipse工程：执行sbt eclipse，即可自动创建出eclipse开发工程。

打包工程：执行 sbt package，打成的jar包不包含外部引用的jar包；执行 sbt assembly，将应用的jar包解压后和工程代码打包为一个jar包，可以执行部署运行了。
		
## 启动/停止
执行sbin/start.sh sbin/stop.sh
