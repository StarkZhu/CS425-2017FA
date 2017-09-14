for i in `seq 1 9`
do
#    ssh-copy-id ${USER}@fa17-cs425-g29-0${i}.cs.illinois.edu
    ssh -n -f ${USER}@fa17-cs425-g29-0${i}.cs.illinois.edu "sh -c 'cd /home/cs425/MP1; nohup python3.6 server.py > /dev/null 2>&1 &'"
done

# ssh-copy-id ${USER}@fa17-cs425-g29-10.cs.illinois.edu
ssh -n -f ${USER}@fa17-cs425-g29-10.cs.illinois.edu "sh -c 'cd /home/cs425/MP1; nohup python3.6 server.py > /dev/null 2>&1 &'"

wait
