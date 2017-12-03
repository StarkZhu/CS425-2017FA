for i in `seq 1 7`;
do
    for j in `seq 1 3`;
    do
        ./run_graphx.sh PageRankExample ${i}
        sleep 120
    done
done