1. 开发、打包工程：
（1）构建eclipse工程：执行sbt eclipse，即可自动创建出eclipse开发工程
（2）打包工程：执行 sbt package，打成的jar包不包含外部引用的jar包
		执行 sbt assembly，将应用的jar包解压后和工程代码打包为一个jar包，可以执行部署运行了
		
2.启动/停止：见sbin/start.sh sbin/stop.sh