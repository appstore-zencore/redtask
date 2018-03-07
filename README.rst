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

    application:
        daemon: true
        pidfile: /tmp/app.pid
    task-server:
        queue: qname
        threads: 10
        handler: example.task.handler
        pull-timeout: 1
        redis:
            url: redis://localhost/0
            options:
                retry_on_timeout: true
                decode_responses: true
        prefix: "ansible-gateway:"
        worker:
            name: 1a3c1921-07b5-476e-a53a-3a1f53b676a5
            expire: 30

