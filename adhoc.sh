for i in `seq 1 9`;
do
    ssh ${USER}@fa17-cs425-g29-0${i}.cs.illinois.edu "cd /home/cs425/MP1; wget https://courses.engr.illinois.edu/cs425/fa2017/CS425_MP1_Demo_Logs_FA17/vm${i}.log" 
ssh ${USER}@fa17-cs425-g29-0${i}.cs.illinois.edu "rm /home/cs425/MP1/*.log.1" 
done

ssh ${USER}@fa17-cs425-g29-10.cs.illinois.edu "cd /home/cs425/MP1; wget https://courses.engr.illinois.edu/cs425/fa2017/CS425_MP1_Demo_Logs_FA17/vm10.log" 
ssh ${USER}@fa17-cs425-g29-10.cs.illinois.edu "rm /home/cs425/MP1/*.log.1"  
wait
