ssh-keygen -t rsa -b 4096 -C "your_email@example.com"

for i in `seq 1 9`;
do
	scp -r .ssh ${USER}@fa17-cs425-g29-0${i}.cs.illinois.edu:~ &
done

scp -r .ssh ${USER}@fa17-cs425-g29-10.cs.illinois.edu:~ &

wait
