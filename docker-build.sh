#!/bin/bash

NAMESPACE_NAME=obaidul
AUTOMATION_APP_VERSION=1.4
AUTOMATION_APP_REPO=google-cloud-automation

NOTIFICATION_APP_VERSION=1.4
NOTIFICATION_APP_REPO=smalldb-notification-service

echo "Building $AUTOMATION_APP_REPO"
sudo docker build -f Dockerfile . -t $NAMESPACE_NAME/$AUTOMATION_APP_REPO:$AUTOMATION_APP_VERSION
sudo docker build -f Dockerfile . -t $NAMESPACE_NAME/$AUTOMATION_APP_REPO:latest

echo "Building $NOTIFICATION_APP_REPO"
sudo docker build -f Dockerfile-notification . -t $NAMESPACE_NAME/$NOTIFICATION_APP_REPO:$NOTIFICATION_APP_VERSION
sudo docker build -f Dockerfile-notification . -t $NAMESPACE_NAME/$NOTIFICATION_APP_REPO:latest

echo "Pushing $AUTOMATION_APP_REPO"
sudo docker push $NAMESPACE_NAME/$AUTOMATION_APP_REPO:$AUTOMATION_APP_VERSION
sudo docker push $NAMESPACE_NAME/$AUTOMATION_APP_REPO:latest

echo "Pushing $NOTIFICATION_APP_REPO"
sudo docker push $NAMESPACE_NAME/$NOTIFICATION_APP_REPO:$NOTIFICATION_APP_VERSION
sudo docker push $NAMESPACE_NAME/$NOTIFICATION_APP_REPO:latest

