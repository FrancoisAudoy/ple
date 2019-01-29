spark-submit \
    --deploy-mode client \
    --master local \
    --num-executors 2 \
    --executor-memory 1024M \
    --executor-cores 2 \
    --class bigdata.TPSpark \
    $1
