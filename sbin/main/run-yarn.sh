
# -p 并行度 5
# -yn taskmanager  yarn 的 container 的数量 4
# -yjm JobManager 内存
# -ytm taskManager 内存
# -ys 每个 taskManager 的 slot数量 2
# -s hdfs://Stream2/user/marketplus/ssp-flink/checkpoint/806c6e5744ac8e3cbd2d2afdc0a09cf6/chk-4
# flink 10 去掉 -yn参数，taskmanager的个数由 (并行度/slot) +1 决定 否则报 Could not build the program from JAR file.
# 同时 flink需要hadoop的shade包放在 flink/lib下
# https://mvnrepository.com/artifact/org.apache.flink/flink-shaded-hadoop-2-uber
/home/marketplus/flink-1.10.1/bin/flink run \
 -m yarn-cluster  \
 --yarnname Adplatform_ssp_flink_report_${MOBSSP_ENV} \
 -yD env.java.opts=$javaopt \
 -p 5 -yjm 1024 -ytm 1024 -ys 2 \
 -c com.mob.adplat.entry.MobsspFlinkStreamReport2ESEntry \
 $stream_jar $profilePath