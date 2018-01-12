	/opt/spark/bin/spark-submit   \
	--class poc.streaming.stream.kafka.StatefulKafkaToHDFS  \
	--master yarn  \
	--driver-memory 8g \
    --executor-memory 4g \
	--deploy-mode client   \
	/media/elevy/win_linux_shared/dev/git/bigdata/spark/target/spark-1.0.jar