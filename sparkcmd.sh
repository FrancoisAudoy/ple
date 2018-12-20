spark-submit \
    --deploy-mode client \
    --master local \
    --num-executors 24 \
    --executor-memory 2048M \
    --executor-cores 2 \
    --class bigdata.TPSpark \
    $1
