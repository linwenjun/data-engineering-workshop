#!/usr/bin/env bash
sbt package
docker exec spark-demo_spark_1 bin/spark-submit --class workshop.wordcount.Product --master local[2]   --packages com.typesafe:config:1.3.2   /jars/data-engineering-workshop_2.11-0.1.jar a b