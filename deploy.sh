sudo chmod -R 777 .
for i in `seq 2 9`;
do
	scp -r . ${USER}@fa17-cs425-g29-0${i}.cs.illinois.edu:/home/cs425 
        ssh -n -f ${USER}@fa17-cs425-g29-0${i}.cs.illinois.edu "sh -c 'chmod -R 777 /home/cs425'"
done

scp -r . ${USER}@fa17-cs425-g29-10.cs.illinois.edu:/home/cs425
ssh -n -f ${USER}@fa17-cs425-g29-10.cs.illinois.edu "sh -c 'chmod -R 777 /home/cs425'"

wait
