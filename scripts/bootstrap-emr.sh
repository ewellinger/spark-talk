#!/bin/bash

# Record starting time
touch $HOME/.bootstrap-begin

sudo yum -y update
sudo yum -y install tmux

# set -e
wget -S -T 10 -t 5 https://repo.continuum.io/archive/Anaconda2-4.2.0-Linux-x86_64.sh -O $HOME/anaconda.sh
sudo bash $HOME/anaconda.sh -b -p $HOME/anaconda
sudo rm $HOME/anaconda.sh
sudo chown -R hadoop:hadoop $HOME/anaconda
export PATH=$HOME/anaconda/bin:$PATH

# Add Anaconda to path
echo -e "\n\n# Anaconda2" >> $HOME/.bashrc
echo "export PATH=$HOME/anaconda/bin:$PATH" >> $HOME/.bashrc

# Download spaCy
$HOME/anaconda/bin/conda install -y spacy

# Download boto3
$HOME/anaconda/bin/conda install -y boto3

# Download future package
$HOME/anaconda/bin/conda install -y future

# Download English module to /mnt/spacy_en_data
sudo mkdir /mnt/spacy_en_data
sudo chown -R hadoop:hadoop /mnt/spacy_en_data

ipython -c "import sputnik; \
import spacy; \
sputnik.install('spacy', spacy.about.__version__, 'en', data_path='/mnt/spacy_en_data/')"

# Record ending time
touch $HOME/.bootstrap-end
