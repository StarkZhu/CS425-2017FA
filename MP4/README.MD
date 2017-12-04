This is the implementation of MP4. 

===START THE NODE===

Assuming running in MP4/ folder
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
* sava graph_file app.py params 

===SETUP===
* You may need to run ../setup.sh once to setup. 

===LOGS===

- Logs will be redirected to mp4.log
- Grep leavers: `python3.6 client.py /home/cs425/MP4/mp4.log left`
- Grep join: `python3.6 client.py /home/cs425/MP4/mp4.log joining`
- Grep offline/crash: `python3.6 client.py /home/cs425/MP4/mp4.log offline`

Please let us know if you have any question. {dlei5, qzhu3}@illinois.edu
