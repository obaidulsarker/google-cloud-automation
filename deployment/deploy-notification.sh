#!/bin/bash
LOGFILE=/app/scripts/deploy.log

NAMESPACE_NAME=docker.xyz.com/smalldb

AUTOMATION_APP_REPO=notification
AUTOMATION_APP_VERSION=latest


echo "$(date) : [$AUTOMATION_APP_REPO] deployment is Started." >> $LOGFILE;
sudo docker ps --filter status=exited -q | xargs docker rm
sudo docker pull $NAMESPACE_NAME/$AUTOMATION_APP_REPO:$AUTOMATION_APP_VERSION
sudo docker run -d -v /app/logs:/app/logs -v /app/cred:/app/cred -v /app/data:/app/data -v /app/conf:/app/conf $NAMESPACE_NAME/$AUTOMATION_APP_REPO:$AUTOMATION_APP_VERSION
echo "$(date) : [$AUTOMATION_APP_REPO] deployment is Completed." >> $LOGFILE;
