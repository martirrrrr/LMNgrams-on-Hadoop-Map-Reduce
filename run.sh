
#!/bin/bash
cat README
function is_valid_integer {
    re='^[0-9]+$'
    if ! [[ $1 =~ $re ]] ; then
        return 1
    fi
    if [ $1 -le 1 ]; then
        return 1
    fi
    return 0
}

if [ $# -eq 0 ] || ! is_valid_integer "$1"; then
    echo "Invalid input. Set n=2 as default."
    n=2
    m=1
else
    n=$1
    m=$((n-1))
fi
echo "N:"$n "N-1:"$m

# Esegui i comandi per avviare il codice:
echo "Run MapReduce for" $n"-gram count"
javac -cp `hadoop classpath` *.java
jar cf ng.jar NGramCounter*.class
export HADOOP_CLASSPATH=ng.jar
hadoop jar ng.jar NGramCounter /user/MapReduce/input/Ngrams/dataset.txt /user/MapReduce/output/Ngrams $n 0
hdfs dfs -cat /user/MapReduce/output/Ngrams/part-r-00002 /user/MapReduce/output/Ngrams/part-r-00001 /user/MapReduce/output/Ngrams/part-r-00000 | hdfs dfs -put - /user/MapReduce/input/Ngrams/ngrams.txt
hdfs dfs -cat /user/MapReduce/input/Ngrams/ngrams.txt

echo "Run MapReduce for" $m"-gram count"
javac -cp `hadoop classpath` *.java
jar cf ng.jar NGramCounter*.class
export HADOOP_CLASSPATH=ng.jar
hadoop jar ng.jar NGramCounter /user/MapReduce/input/Ngrams/dataset.txt /user/MapReduce/output/N-1grams $m 1
hdfs dfs -cat /user/MapReduce/output/N-1grams/part-r-00002 /user/MapReduce/output/N-1grams/part-r-00001 /user/MapReduce/output/N-1grams/part-r-00000 | hdfs dfs -put - /user/MapReduce/input/Ngrams/n-1grams.txt
hdfs dfs -cat /user/MapReduce/input/Ngrams/n-1grams.txt


echo "Run Conditional Probability calculator"
javac -cp `hadoop classpath` *.java
jar cf cpc.jar CPCalculator*.class
export HADOOP_CLASSPATH=cpc.jar
hadoop jar cpc.jar CPCalculator $n
echo “Bigram Probability results:”
hdfs dfs -cat /user/MapReduce/input/Ngrams/probability.txt
