#!/bin/bash

# --------------------------------------------------------------------------------------
# Install Anaconda and its dependencies

# Download Anaconda
wget https://repo.anaconda.com/archive/Anaconda3-2023.03-1-Linux-x86_64.sh /home/rob/Downloads

# Install dependencies
# sudo apt-get install libgl1-mesa-glx libegl1-mesa libxrandr2 libxrandr2 libxss1 libxcursor1 libxcomposite1 libasound2 libxi6 libxtst6

# Change authorizations
shasum -a 256 /home/rob/Downloads/Anaconda3-2023.03-1-Linux-x86_64.sh
# Replace /PATH/FILENAME with your installation's path and filename.

# Execute shell script
bash /home/rob/Downloads/Anaconda3-2023.03-1-Linux-x86_64.sh
