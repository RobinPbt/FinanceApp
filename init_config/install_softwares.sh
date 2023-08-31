#!/bin/bash

sudo apt-get install sqlite3

sudo apt-get install git-all
git config --global user.name RobinPbt
git config --global user.email 104992181+RobinPbt@users.noreply.github.com
git clone https://github.com/RobinPbt/FinanceApp.git
cd FinanceApp/
git remote add FinApp https://github.com/RobinPbt/FinanceApp.git
