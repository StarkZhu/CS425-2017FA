This is the implementation of MP3. 

===START THE NODE===

Assuming running in MP3/ folder
`python3.6 main.py`



===SUPPORTED COMMAND===

* join send request to join
* leave leave the system gracefully
* lsm list members
* lss list self's address 
* put localfilename sdfsfilename
* get sdfsfilename localfilename
* delete sdfsfilename
* ls sdfsfilename
* store

===SETUP===
* You may need to run ../setup.sh once to setup. 

===LOGS===

- Logs will be redirected to mp2.log
- Grep leavers: `python3.6 client.py /home/cs425/MP2/mp2.log left`
- Grep join: `python3.6 client.py /home/cs425/MP2/mp2.log joining`
- Grep offline/crash: `python3.6 client.py /home/cs425/MP2/mp2.log offline`

Please let us know if you have any question. {dlei5, qzhu3}@illinois.edu
