# file: .profile.d/ssh-setup.sh

#!/bin/bash
echo $0: creating public and private key files

# Create the .ssh directory
mkdir -p ${HOME}/.ssh
chmod 700 ${HOME}/.ssh

# Create the public and private key files from the environment variables.
echo "${EC2_PUBLIC_KEY}" > ${HOME}/.ssh/heroku_id_rsa.pub
chmod 644 ${HOME}/.ssh/heroku_id_rsa.pub

# Note use of double quotes, required to preserve newlines
echo "${EC2_PRIVATE_KEY}" > ${HOME}/.ssh/heroku_id_rsa
chmod 600 ${HOME}/.ssh/heroku_id_rsa

# Preload the known_hosts file  (see "version 2" below)

# Start the SSH tunnel if not already running
SSH_CMD="ssh -o StrictHostKeyChecking no -f -i ${HOME}/.ssh/heroku_id_rsa -N -L 5433:${EC2_REMOTE_HOST}:6432 ${EC2_TUNNEL_USER}@${EC2_TUNNEL_SITE}"
PID=`pgrep -f "${SSH_CMD}"`
if [ $PID ] ; then
    echo $0: tunnel already running on ${PID}
else
    echo $0 launching tunnel
    $SSH_CMD
fi
