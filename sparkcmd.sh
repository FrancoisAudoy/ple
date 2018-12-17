spark-submit \
      --deploy-mode client \
      --master local \
      --num-executors 20 \
      --executor-memory 2048M \
      --executor-cores 4 \
      --class bigdata.TPSpark \
      $1
