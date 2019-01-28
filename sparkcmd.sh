spark-submit \
    --deploy-mode cluster \
    --master yarn \
    --num-executors 10 \
    --executor-memory 4096M \
    --executor-cores 2 \
    --class bigdata.TPSpark \
    $1
