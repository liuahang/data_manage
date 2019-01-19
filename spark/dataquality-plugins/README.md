1.数据质量插件

包含空值检测和正则表达式检测

2.打包 

正则表示式: mvn clean compile scala:compile package -DskipTests -Pregex 

空值:clean compile scala:compile package -DskipTests -Pnull