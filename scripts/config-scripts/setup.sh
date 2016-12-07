    #!/usr/bin/env bash

    # This is an automatic setup script to install medusa-2 from scratch

    echo "==> Creating keys"
    ssh-keygen -t rsa

    sudo apt-get update
    sudo apt-get install git

    echo "==> Cloning medusa"
    mkdir Programs
    cd ~/Programs
    git clone https://bitbucket.org/pcosta_pt/medusa-1.0
    cd ~/Programs/medusa-1.0
    git pull origin dev

    echo "==> Installing medusa 1.0"
    ~/Programs/medusa-1.0/scripts/config-scripts/ec2-install.sh

    echo "==> 1. Run the configure-files.sh NAMENODE_HOST"
    echo "==> 2. Copy all ssh public keys from all hosts to ~/.ssh/authorized_keys"
    echo "==> 3. Configure rabbitmq-server"
    echo "==> 4. Check if hosts are well configured in /etc/hosts"
    echo "==> 5. Install python plugins in the master with ./scripts/config-scripts/pip-plugins.sh"
    echo "==> 6. Set the max and the min of JVM in mapred-site.xml to the params of your host, if necessary"
