Here are the steps to set up Docker on WSL2:

1. **Install and configure WSL**: Open the command line as administrator and run the following commands to install WSL:

```bash
dism.exe /online /enable-feature /featurename:VirtualMachinePlatform /all /norestart
dism.exe /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart
```
After the operation finishes, restart Windows. Then, open the command line as administrator, and run this command: `wsl --set-default-version 2`.

2. **Install Ubuntu 22 LTS**: Install Ubuntu via MS Store. After installation, you should see a new window with Linux open. If you don't, restart Windows and retry the Ubuntu installation.

3. **Install Docker**: Open a new terminal tab and run the following commands:

```bash
. /etc/os-release
curl -fsSL https://download.docker.com/linux/${ID}/gpg | sudo tee /etc/apt/trusted.gpg.d/docker.asc
echo "deb [arch=amd64] https://download.docker.com/linux/${ID} ${VERSION_CODENAME} stable" | sudo tee /etc/apt/sources.list.d/docker.list
sudo apt update
sudo apt install docker-ce docker-ce-cli containerd.io
sudo service docker start
sudo usermod -a -G docker $USER
```

Then, log out and log back in.

4. **Configure Docker**: Open the nano text editor with `sudo nano /etc/docker/daemon.json` and paste the following at the end of the file: `"hosts": ["unix:///mnt/wsl/shared-docker/docker.sock"]`. Press ctrl-s to save, then ctrl-x to close nano. Start Docker with `sudo dockerd`. If it doesn't work, it should display the PID of the previous process. Use it in the command below: `sudo kill PID` then `sudo dockerd`.

5. **Test Docker**: Open a new terminal tab and test the config with the following commands:

```bash
export DOCKER_HOST="unix:///mnt/wsl/shared-docker/docker.sock"
docker run --rm hello-world
```
6. **Set Docker to autostart**: Open the nano text editor with ` nano .bashrc` and paste the following lines at the end of the file:

```bash
#Docker autostart
DOCKER_DISTRO="your_distro_name" #run “wsl -l -q” in Powershell
DOCKER_DIR=/mnt/wsl/shared-docker
DOCKER_SOCK="$DOCKER_DIR/docker.sock"
export DOCKER_HOST="unix://$DOCKER_SOCK"
if [ ! -S "$DOCKER_SOCK" ]; then
  mkdir -pm o=,ug=rwx "$DOCKER_DIR"
  chgrp docker "$DOCKER_DIR"
fi
```
Then, save and close the file.

6. **Allow passwordless access to Docker (optional)**: If when opening terminal, it starts to prompt you for the password each time, run:

```bash
sudo visudo
```
Paste the lines below at the end of the file:
```bash
#allow passwordless access to Docker
%docker ALL=(ALL)  NOPASSWD: /usr/bin/dockerd
```
Press ctrl-s to save, then ctrl-x to close nano.
