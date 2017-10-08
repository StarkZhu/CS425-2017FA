for i in `seq 3 9`;
do
    ssh ${USER}@fa17-cs425-g29-0${i}.cs.illinois.edu "pkill python3.6" &
done

ssh ${USER}@fa17-cs425-g29-10.cs.illinois.edu "pkill python3.6" &

wait
