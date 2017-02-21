## Developing TIleDB with Vagrant

Vagrant is a system that creates lightweight, self-contained development
environments as virtual machines. This directory contains a Vagrantfiles
that provision a headless VirtualBox instances for building TileDB.

### Requirements

To develop under Vagrant, you will first need to install [Oracle
VirtualBox](https://www.virtualbox.org/wiki/Downloads) and
[Vagrant](http://downloads.vagrantup.com/). Then, from a command line, enter
the `TileDB/vagrant/*` directory, and enter:

```
$ vagrant up
```

A virtual machine will be downloaded if needed, created, and provisioned with
the dependencies to build TileDB. 

To re-provision the virtual machine (re-run bootstrap.sh):

```
$ vagrant up --provision
```

By default, it exposes an SSH server to your
local machine on port 2222.

```
$ vagrant ssh
```

To bring down the virtual machine, use `suspend` or `halt`.

```
$ vagrant suspend\halt
```

See the [Vagrant
documentation](http://docs.vagrantup.com/v2/) for complete details on using
Vagrant.
