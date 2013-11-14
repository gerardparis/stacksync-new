StackSync Synchronization service
=================================


# Requirements

* Java 1.6 or newer
* Maven 2
* PostgreSQL 9.2
* RabbitMQ

# Installing Maven 2

To install Maven 2 you only need to run the following command.

    $ sudo apt-get install maven2


# Installing RabbitMQ

To install the latest RabbitMQ you only need to run the following command.

    $ sudo apt-get install rabbitmq-server



# Installing PostgreSQL

First, you need to install the required libs using the command below:

    $ sudo apt-get install libpq-dev


Open a terminal and run the following commands to add the repository containing PostgreSQL 9.2 and install it:

    $ sudo add-apt-repository ppa:pitti/postgresql
    $ sudo apt-get update
    $ sudo apt-get install postgresql-9.2
    $ sudo apt-get upgrade


# Database initialization

In order to initialize the database we need to create the database and the user and execute the script “setup_db.sql” located in "src/main/resources".

First, enter in a Postgres command line mode:

    $ sudo -u postgres psql

Execute the commands below to create a user and the database. The database must be called stacksync:

    postgres=# create database stacksync;
    postgres=# create user stacksync_user with password 'mysecretpwd';
    postgres=# grant all privileges on database stacksync to stacksync_user;
    postgres=# \q

Enter to the database with the user role created. Note that the first parameter is the host, the second is the database name and the last one is the username:

    $ psql -h localhost stacksync stacksync_user

Now run execute the script.

    postgres=# \i ./setup_db.sql
    postgres=# \q


# Create a new user

Go to the "script" folder inside the SyncService project and run the following command to install the necessary tools to create users.

    $ sudo ./install.sh

Now run the following commands to add a new user and a new workspace associated to the user. The user must be the same as the one in the storage backend.

    $ ./postgres/adduser.rb -i <USER> -n <USER> -q 1
    $ ./postgres/addworkspace.rb -i <USER> -p /



# COMPILING THE STACKSYNC SYNCSERVICE PROJECT

First of all, you need to manually add the ObjectMQ library into your local Maven repository. Assuming you are located in the root folder of the project, just run the following command:

    $ mvn install:install-file -Dfile=lib/objectMQ.jar -DgroupId=objectmq -DartifactId=objectmq -Dversion=1.0 -Dpackaging=jar

Now we just need to assemble the project into a JAR:

    $ mvn assembly:assembly

This will generate a "target" folder containing a JAR file called "syncservice-X.X-jar-with-dependencies.jar"

> NOTE: if you get an error (BUILD FAILURE), run the following command to clean your local repository and execute the commands of this section again.

    $ rm -rf ~/.m2/repository/*


# CONFIGURING THE SYNCSERVICE

Now we need to generate the properties file you can just run the JAR with the argument --dump-config and redirect the output to a new file:

    $ java -jar syncservice-X.X-jar-with-dependencies.jar --dump-config > config.properties

This will generate a file called "config.properties" where the different parameters can be set up.


# EXECUTING THE SYNCSERVICE

You only need to run the following command specifying the location of your configuration file.

    $ java -jar syncservice-0.3-jar-with-dependencies.jar --config config.properties


