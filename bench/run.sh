echo Warmup for 10s
hey -z 10s -cpus 4 -c 1000 http://192.168.0.8:8080;

echo 10s
hey -z 10s -cpus 4 -c 100 http://192.168.0.8:8080;
hey -z 10s -cpus 4 -c 1000 http://192.168.0.8:8080;


echo 5m
hey -z 5m -cpus 4 -c 100 http://192.168.0.8:8080;
hey -z 5m -cpus 4 -c 1000 http://192.168.0.8:8080;
