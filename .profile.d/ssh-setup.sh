# file: .profile.d/ssh-setup.sh

#!/bin/bash
echo $0: creating public and private key files

# Create the .ssh directory
# mkdir -p ${HOME}/.ssh
# chmod 700 ${HOME}/.ssh

# Create the public and private key files from the environment variables.
source <(grep = '/mnt/c/Users/ShriramSunder/Projects/streams/config.ini')

chmod 644 ${HOME}/.ssh/new_ts_pair_openssh.pub

# Note use of double quotes, required to preserve newlines
chmod 600 ${HOME}/.ssh/new_ts_pair_openssh

# Start the SSH tunnel if not already running
SSH_CMD="ssh -v -f -i ${HOME}/.ssh/new_ts_pair_openssh -N -L 5433:127.0.0.1:6432 ${ssh_user}@${ssh_host} -o StrictHostKeyChecking=no"
PID=`pgrep -f "${SSH_CMD}"`
if [ $PID ] ; then
    echo $0: tunnel already running on ${PID}
else
    echo $0 launching tunnel
    $SSH_CMD
fi
