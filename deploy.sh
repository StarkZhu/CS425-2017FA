# sudo chmod -R 777 .
rm /home/cs425/MP1/machine.*.log
for i in `seq 2 9`;
do
	# ssh -n -f ${USER}@fa17-cs425-g29-0${i}.cs.illinois.edu "sh -c 'mkdir /home/cs425/MP3/sdfs'"
	scp -r /home/cs425/MP4/*.py ${USER}@fa17-cs425-g29-0${i}.cs.illinois.edu:/home/cs425/.  &
    ssh -n -f ${USER}@fa17-cs425-g29-0${i}.cs.illinois.edu "sh -c 'chmod -R 777 /home/cs425/MP4'" &
done

# ssh -n -f ${USER}@fa17-cs425-g29-10.cs.illinois.edu "sh -c 'mkdir /home/cs425/MP3/sdfs'"

scp -r /home/cs425/MP4/*.py ${USER}@fa17-cs425-g29-10.cs.illinois.edu:/home/cs425/.
ssh -n -f ${USER}@fa17-cs425-g29-10.cs.illinois.edu "sh -c 'chmod -R 777 /home/cs425/MP4'"

wait
