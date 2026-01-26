#!/bin/bash

export_env_vars() {
    echo "Exporting environment variables..."
    printenv | grep -E '^RUNPOD_|^PATH=|^_=' | awk -F = '{ print "export " $1 "=\"" $2 "\"" }' >> /etc/rp_environment
    echo 'source /etc/rp_environment' >> ~/.bashrc

    # Export RUNPOD_S3_* variables as S3_* variables
    printenv | grep -E '^RUNPOD_S3_' | while IFS='=' read -r key value; do
        s3_key="${key#RUNPOD_S3_}"
        export "S3_${s3_key}"="${value}"
        echo "export S3_${s3_key}=\"${value}\"" >> /etc/rp_environment
    done

    echo 'source /etc/rp_environment' >> ~/.bashrc
}

# System setup script
# Installs required packages and configures git-lfs

set -e

export_env_vars

# Check if /app/cnode/start.sh already exists
if [ -f /app/cnode/start.sh ]; then
    echo "/app/cnode/start.sh already exists, running it directly..."
    cd /app/cnode
    ./start.sh
    exit 0
fi

echo "Updating package lists..."
apt-get update

echo "Installing packages..."

apt-get update && apt-get install -y \
      fonts-dejavu-core rsync git git-lfs jq moreutils aria2 wget curl \
      libglib2.0-0 libsm6 libgl1 libxrender1 libxext6 ffmpeg bc \
      libgoogle-perftools4 libtcmalloc-minimal4 procps software-properties-common \
      build-essential cmake ninja-build libjpeg-dev libeigen3-dev \
      libwebp-dev zlib1g-dev libpng-dev

echo "Cleaning up..."
apt-get autoremove -y
apt-get remove -y python3-blinker
apt-get clean
rm -rf /var/lib/apt/lists/*

echo "Installing git-lfs..."
git lfs install

echo "Creating /app directory..."
mkdir -p /app

echo "Creating Python virtual environment..."
cd /app
python3.11 -m venv installvenv

echo "Activating virtual environment..."
source installvenv/bin/activate

echo "Installing Python packages..."
pip install tqdm
pip install boto3

echo "Downloading s3_download.py script..."
wget -O /app/s3_download.py https://raw.githubusercontent.com/Wolverinoid/runpods-comfy/refs/heads/main/s3_download.py
chmod +x /app/s3_download.py

/app/installvenv/bin/python /app/s3_download.py runpods/video-node.tar.gz /app/ --workers 12 --chunk-size 64

cd /app

tar -zxvf video-node.tar.gz

rm video-node.tar.gz

deactivate

cd /app/cnode

python3.11 -m venv --upgrade /app/cnode/trellisvenv
python3.11 -m venv --upgrade /app/external/LTX-2/.venv

echo "Setup complete!"

./start.sh

#echo "Keeping container alive..."
#tail -f /dev/null