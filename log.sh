#!/bin/bash

nginx_home=/opt/module/nginx
server_home=/opt/gmall1130


case $1 in
"start")
# 先启动nginx

if [[ -z "`ps -ef | awk '/nginx/ && !/awk/ {print \$n}'`" ]]; then
    echo "在hadoop162上启动nginx"
    $nginx_home/sbin/nginx
else
    echo "nginx已经启动, 无需重复启动"
fi

# 分别在三台设备启动日志服务器
for host in hadoop162 hadoop163 hadoop164 ; do
    echo "在 $host 上启动日志服务器"
    ssh $host "nohup java -jar $server_home/gmall-logger-0.0.1-SNAPSHOT.jar  1>$server_home/logs.log 2>&1 &"
done

;;
"stop")
echo "在hadoop162上停止nginx"
$nginx_home/sbin/nginx -s stop
for host in hadoop162 hadoop163 hadoop164 ; do
    echo "在 $host 上停止日志服务器"
    ssh $host "jps | awk '/gmall-logger-0.0.1-SNAPSHOT.jar/  {print \$1}' | xargs kill -9"
done
;;
*)
echo "log.sh start 启动日期采集服务器"
echo "log.sh stop  停止动日期采集服务器"
;;
esac


