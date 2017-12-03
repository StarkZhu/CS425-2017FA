for i in `seq 2 9`;
do
    ssh ${USER}@fa17-cs425-g29-0${i}.cs.illinois.edu "pkill pypy3" &
done

#ssh ${USER}@fa17-cs425-g29-03.cs.illinois.edu "pkill python3.6" &
#ssh ${USER}@fa17-cs425-g29-05.cs.illinois.edu "pkill python3.6" &
#ssh ${USER}@fa17-cs425-g29-07.cs.illinois.edu "pkill python3.6" &

ssh ${USER}@fa17-cs425-g29-10.cs.illinois.edu "pkill pypy3" &

wait
