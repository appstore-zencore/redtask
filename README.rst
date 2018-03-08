redtask
=======

.. image:: https://www.travis-ci.org/appstore-zencore/redtask.svg?branch=master
    :target: https://www.travis-ci.org/appstore-zencore/redtask

Redis task manager.


Install
-------

::

    pip install redtask


Usage
-----


Example Config
--------------

::

    services:
        debug.ping: redtask.debug.ping
        debug.echo: redtask.debug.echo
    task-server:
        name: ctrlstack
        queue-name: run-ansible-playbook
        pool-size: 30
        pull-timeout: 1
        node:
            name: unittest
            keepalive: 3
        redis:
            url: redis://localhost/0
            options:
            retry_on_timeout: true
            decode_responses: true
