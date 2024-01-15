# Python Virtual Environment
To properly work in an isolated environment we are using a Python virtual environment.

## First: Install Java
### Linux
```shell script
$ sudo apt install openjdk-8-jdk
OR..
$ sudo yum install java-1.8.0-openjdk (depends on the distro)
...

$ java -version
openjdk version "1.8.0_282"
OpenJDK Runtime Environment (build 1.8.0_282-8u282-b08-0ubuntu1~20.04-b08)
OpenJDK 64-Bit Server VM (build 25.282-b08, mixed mode)
```

### Mac
```shell script
$ brew tap adoptopenjdk/openjdk

brew install --cask adoptopenjdk8
# brew install --cask adoptopenjdk9
# brew install --cask adoptopenjdk10
# brew install --cask adoptopenjdk11
...

$ java -version
openjdk version "1.8.0_292"
OpenJDK Runtime Environment (AdoptOpenJDK)(build 1.8.0_292-b10)
OpenJDK 64-Bit Server VM (AdoptOpenJDK)(build 25.292-b10, mixed mode)
```


### Java Home
Add after the PATH line in .bashrc: 
* `export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/`
* `export JAVA_HOME=/usr/lib/jvm/jre-1.8.0-openjdk/`

#### Java Home in Mac
From: https://stackoverflow.com/questions/24342886/how-to-install-java-8-on-mac

```shell script
$  brew install jenv
...
To activate jenv, add the following to your ~/.zshrc:
  export PATH="$HOME/.jenv/bin:$PATH"
  eval "$(jenv init -)"

$ source .zshrc

# Add the installed java to jenv:
$ jenv add /Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home
openjdk64-1.8.0.292 added
1.8.0.292 added
1.8 added

# To see all the installed java:
$ jenv versions
* system (set by /Users/facundoradrizzani/.jenv/version)
  1.8
  1.8.0.292
  openjdk64-1.8.0.292
  
# Configure the java version which you want to use:
$ jenv global openjdk64-1.8.0.292

# To set JAVA_HOME:
$ jenv enable-plugin export
```

## Python Virtual Environment
1) Check if `virtualenv` package if installed:
```shell
$ virtualenv --version

# If not installed, run:
$ pip install virtualenv
```

2) Go to the desire location to create a folder with the VE packages

3) (Optional) Install a different Python version than the core system with `pyenv`

Using a different Python version:
```shell
$ python3 -V
 Python 3.11.3
$ ls ~/.pyenv/versions/
3.10.3 3.7.5  3.9.11

# If I want to install a new version
$ vpyenv install -v 3.11.7

$ ls ~/.pyenv/versions/
3.10.3 3.11.7 3.7.5  3.9.11
```

Using the new version to create the virtualenv:
```shell
$ virtualenv -p ~/.pyenv/versions/3.11.7/bin/python p1env

$ source p1env/bin/activate
(p1env) $ python -V
Python 3.11.7
```

We are now in a new virtual environment and is safe to insalla/unistall and delete packages without affecting the core Python installation.

## Installing the needed packages
```shell
(p1env) $ pip list
Package    Version
---------- -------
pip        23.3.2
setuptools 57.0.0
wheel      0.36.2
```

The best is to use the _requirements.txt_ file instead of installing all the stuff manually.
I've create a large file with a lot of packages, may be not all of them are necessary.
(p1env) $ pip install -r requirements.txt

```shell
pip install jupyter
pip install pgcli
pip install pandas
pip install sqlalchemy
pip install psycopg2
```
