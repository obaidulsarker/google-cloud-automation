#!/bin/bash
LOGFILE=deploy.log
echo "$(date) : Deployment is Started." >> $LOGFILE
sudo docker ps --filter status=exited -q | xargs docker rm
sudo docker pull obaidul/google-cloud-automation:1.0 2>> $LOGFILE
sudo docker run -v /app/logs:/app/logs -v /app/cred:/app/cred obaidul/google-cloud-automation:1.0 2>> $LOGFILE
echo "$(date) : Deployment is Completed." >> $LOGFILE;
