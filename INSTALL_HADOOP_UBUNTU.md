# How to Install Hadoop on Ubuntu

Follow these steps to install Hadoop (Single Node / Local Mode) on Ubuntu.

## 1. Install Java 8 (Required for Hadoop compatibility)

While our project runs on Java 17, standard Hadoop builds are often most stable with Java 8 for the runtime environment. However, since we are using `hadoop` command line just to submit jobs, let's ensure we have a compatible version.

```bash
sudo apt update
sudo apt install openjdk-8-jdk -y
```

## 2. Download and Extract Hadoop

We will download Hadoop 3.3.6 (a stable version).

```bash
cd ~
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz
mv hadoop-3.3.6 hadoop
```

## 3. Configure Environment Variables

Add the Hadoop paths to your shell configuration (`.bashrc`).

```bash
nano ~/.bashrc
```

Scroll to the bottom and add these lines:

```bash
# Hadoop Variables
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=~/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
```

Save and exit (`Ctrl+O`, `Enter`, `Ctrl+X`).

Then apply the changes:

```bash
source ~/.bashrc
```

## 4. Configure `hadoop-env.sh`

We need to tell Hadoop explicitly where Java is.

```bash
nano ~/hadoop/etc/hadoop/hadoop-env.sh
```

Find the line `export JAVA_HOME=` and change it to:

```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
```

Save and exit.

## 5. Verify Installation

Run:

```bash
hadoop version
```

You should see "Hadoop 3.3.6".

## 6. Run the Project

Now you can go back to the project folder and run:

```bash
./run_project.sh
```
