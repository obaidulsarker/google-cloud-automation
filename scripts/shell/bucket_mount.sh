
#!/bin/bash

# Replace these variables with your specific values
BUCKET_NAME="bits-database-backup"
MOUNT_POINT="/home/obaidul_1432/gcs"

# Install gcsfuse (if not already installed)
if ! command -v gcsfuse &> /dev/null; then
    echo "Installing gcsfuse..."
    sudo tee /etc/yum.repos.d/gcsfuse.repo <<EOF
[gcsfuse]
name=gcsfuse (packages.cloud.google.com)
baseurl=https://packages.cloud.google.com/yum/repos/gcsfuse-el7-x86_64
enabled=1
gpgcheck=1
repo_gpgcheck=1
gpgkey=https://packages.cloud.google.com/yum/doc/yum-key.gpg
       https://packages.cloud.google.com/yum/doc/rpm-package-key.gpg
EOF
    sudo yum -y install gcsfuse
fi

# Create a mount point directory if it doesn't exist
if [ ! -d "$MOUNT_POINT" ]; then
    echo "Creating mount point directory: $MOUNT_POINT"
    sudo mkdir -p $MOUNT_POINT
fi

# Mount the GCS bucket
echo "Mounting GCS bucket '$BUCKET_NAME' to '$MOUNT_POINT'..."
gcsfuse $BUCKET_NAME $MOUNT_POINT

# echo "OUTPUT=$?"

# Check if the mount was successful
if [ $? -eq 0 ]; then
    echo "GCS bucket mounted successfully."
else
    echo "Failed to mount GCS bucket."
fi
