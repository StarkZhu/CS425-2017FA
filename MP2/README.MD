This is the implementation of MP2. 

===START THE NODE===
Assuming running in MP2/ folder
`python3.6 server.py`


===SUPPORTED COMMAND===
* join send request to join
* leave leave the system gracefully
* lsm list members
* lss list self's address 


===LOGS===
Logs will be redirected to mp2.log
Grep leavers: `python3.6 client.py /home/cs425/MP2/mp2.log left`
Grep join: `python3.6 client.py /home/cs425/MP2/mp2.log joining`
Grep offline/crash: `python3.6 client.py /home/cs425/MP2/mp2.log offline`

Please let us know if you have any question. {dlei5, qzhu3}@illinois.edu
