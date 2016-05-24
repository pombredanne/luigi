# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
============================================================
Using ``inherits`` and ``requires`` to ease parameter pain
============================================================

Most luigi plumbers will find themselves in an awkward task parameter situation
at some point or another.  Consider the following "parameter explosion"
problem:

.. code-block:: python

    class TaskA(luigi.ExternalTask):
        param_a = luigi.Parameter()

        def output(self):
            return luigi.LocalTarget('/tmp/log-{t.param_a}'.format(t=self))

    class TaskB(luigi.Task):
        param_b = luigi.Parameter()
        param_a = luigi.Parameter()

        def requires(self):
            return TaskA(param_a=self.param_a)

    class TaskC(luigi.Task):
        param_c = luigi.Parameter()
        param_b = luigi.Parameter()
        param_a = luigi.Parameter()

        def requires(self):
            return TaskB(param_b=self.param_b, param_a=self.param_a)


In work flows requiring many tasks to be chained together in this manner,
parameter handling can spiral out of control.  Each downstream task becomes
more burdensome than the last.  Refactoring becomes more difficult.  There
are several ways one might try and avoid the problem.

**Approach 1**:  Parameters via command line or config instead of ``requires``.

.. code-block:: python

    class TaskA(luigi.ExternalTask):
        param_a = luigi.Parameter()

        def output(self):
            return luigi.LocalTarget('/tmp/log-{t.param_a}'.format(t=self))

    class TaskB(luigi.Task):
        param_b = luigi.Parameter()

        def requires(self):
            return TaskA()

    class TaskC(luigi.Task):
        param_c = luigi.Parameter()

        def requires(self):
            return TaskB()


Then run in the shell like so:

.. code-block:: bash

    luigi --module my_tasks TaskC --param-c foo --TaskB-param-b bar --TaskA-param-a baz


Repetitive parameters have been eliminated, but at the cost of making the job's
command line interface slightly clunkier.  Often this is a reasonable
trade-off.

But parameters can't always be refactored out every class.  Downstream
tasks might also need to use some of those parameters.  For example,
if ``TaskC`` needs to use ``param_a`` too, then ``param_a`` would still need
to be repeated.


**Approach 2**:  Use a common parameter class

.. code-block:: python

    class Params(luigi.Config):
        param_c = luigi.Parameter()
        param_b = luigi.Parameter()
        param_a = luigi.Parameter()

    class TaskA(Params, luigi.ExternalTask):
        def output(self):
            return luigi.LocalTarget('/tmp/log-{t.param_a}'.format(t=self))

    class TaskB(Params):
        def requires(self):
            return TaskA()

    class TaskB(Params):
        def requires(self):
            return TaskB()


This looks great at first glance, but a couple of issues lurk. Now ``TaskA``
and ``TaskB`` have unnecessary significant parameters.  Significant parameters
help define the identity of a task.  Identical tasks are prevented from
running at the same time by the central planner.  This helps preserve the
idempotent and atomic nature of luigi tasks.  Unnecessary significant task
parameters confuse a task's identity.  Under the right circumstances, task
identity confusion could lead to that task running when it shouldn't, or
failing to run when it should.

This approach should only be used when all of the parameters of the config
class, are significant (or all insignificant) for all of its subclasses.

And wait a second... there's a bug in the above code.  See it?

``TaskA`` won't behave as an ``ExternalTask`` because the parent classes are
specified in the wrong order.  This contrived example is easy to fix (by
swapping the ordering of the parents of ``TaskA``), but real world cases can be
more difficult to both spot and fix.  Inheriting from multiple classes
derived from ``luigi.Task`` should be undertaken with caution and avoided
where possible.


**Approach 3**: Use ``inherits`` and ``requires``

The ``inherits`` class decorator in this module copies parameters (and
nothing else) from one task class to another, and avoids direct pythonic
inheritance.

.. code-block:: python

    import luigi
    from luigi.util import inherits

    class TaskA(luigi.ExternalTask):
        param_a = luigi.Parameter()

        def output(self):
            return luigi.LocalTarget('/tmp/log-{t.param_a}'.format(t=self))

    @inherits(TaskA)
    class TaskB(luigi.Task):
        param_b = luigi.Parameter()

        def requires(self):
            t = self.clone(TaskB)

            # Wait... whats this clone thingy do?
            #
            # Pass it a task class.  It calls that task.  And when it does, it
            # supplies all parameters (and only those parameters) common to
            # the caller and callee!
            #
            # The call to clone is equivalent to the following (note the
            # fact that clone avoids passing param_b).
            #
            #   return TaskA(param_a=self.param_a)

            return t

    @inherits(TaskB)
    class TaskC(luigi.Task):
        param_c = luigi.Parameter()

        def requires(self):
            return self.clone(TaskB)


This totally eliminates the need to repeat parameters, avoids inheritance
issues, and keeps the task command line interface as simple (as it can be,
anyway).  Refactoring task parameters is also much easier.

The ``requires`` helper function can reduce this pattern even further.   It
does everything ``inherits`` does, and also attaches a ``requires`` method
to your task (still all without pythonic inheritance).

But how does it know how to invoke the upstream task?  It uses ``clone``
behind the scenes!

.. code-block:: python

    import luigi
    from luigi.util import inherits, requires

    class TaskA(luigi.ExternalTask):
        param_a = luigi.Parameter()

        def output(self):
            return luigi.LocalTarget('/tmp/log-{t.param_a}'.format(t=self))

    @requires(TaskA)
    class TaskB(luigi.Task):
        param_b = luigi.Parameter()

        # The class decorator does this for me!
        # def requires(self):
        #     return self.clone(TaskA)

Use these helper functions effectively to avoid unnecessary
repetition and dodge a few potentially nasty workflow pitfalls at the same
time. Brilliant!
"""

import datetime
import functools
import logging

from luigi import six

from luigi import task
from luigi import parameter
from luigi.deprecate_kwarg import deprecate_kwarg  # NOQA: removing this breaks code

if six.PY3:
    xrange = range

logger = logging.getLogger('luigi-interface')


def common_params(task_instance, task_cls):
    """
    Grab all the values in task_instance that are found in task_cls.
    """
    if not isinstance(task_cls, task.Register):
        raise TypeError("task_cls must be an uninstantiated Task")

    task_instance_param_names = dict(task_instance.get_params()).keys()
    task_cls_param_names = dict(task_cls.get_params()).keys()
    common_param_names = list(set.intersection(set(task_instance_param_names), set(task_cls_param_names)))
    common_param_vals = [(key, dict(task_cls.get_params())[key]) for key in common_param_names]
    common_kwargs = dict([(key, task_instance.param_kwargs[key]) for key in common_param_names])
    vals = dict(task_instance.get_param_values(common_param_vals, [], common_kwargs))
    return vals


def task_wraps(P):
    # In order to make the behavior of a wrapper class nicer, we set the name of the
    # new class to the wrapped class, and copy over the docstring and module as well.
    # This makes it possible to pickle the wrapped class etc.
    # Btw, this is a slight abuse of functools.wraps. It's meant to be used only for
    # functions, but it works for classes too, if you pass updated=[]
    return functools.wraps(P, updated=[])


class inherits(object):
    """
    Task inheritance.

    Usage:

    .. code-block:: python

        class AnotherTask(luigi.Task):
            n = luigi.IntParameter()
            # ...

        @inherits(AnotherTask):
        class MyTask(luigi.Task):
            def requires(self):
               return self.clone_parent()

            def run(self):
               print self.n # this will be defined
               # ...
    """

    def __init__(self, task_to_inherit):
        super(inherits, self).__init__()
        self.task_to_inherit = task_to_inherit

    def __call__(self, task_that_inherits):
        for param_name, param_obj in self.task_to_inherit.get_params():
            if not hasattr(task_that_inherits, param_name):
                setattr(task_that_inherits, param_name, param_obj)

        # Modify task_that_inherits by subclassing it and adding methods
        @task_wraps(task_that_inherits)
        class Wrapped(task_that_inherits):

            def clone_parent(_self, **args):
                return _self.clone(cls=self.task_to_inherit, **args)

        return Wrapped


class requires(object):
    """
    Same as @inherits, but also auto-defines the requires method.
    """

    def __init__(self, task_to_require):
        super(requires, self).__init__()
        self.inherit_decorator = inherits(task_to_require)

    def __call__(self, task_that_requires):
        task_that_requires = self.inherit_decorator(task_that_requires)

        # Modify task_that_requres by subclassing it and adding methods
        @task_wraps(task_that_requires)
        class Wrapped(task_that_requires):

            def requires(_self):
                return _self.clone_parent()

        return Wrapped


class copies(object):
    """
    Auto-copies a task.

    Usage:

    .. code-block:: python

        @copies(MyTask):
        class CopyOfMyTask(luigi.Task):
            def output(self):
               return LocalTarget(self.date.strftime('/var/xyz/report-%Y-%m-%d'))
    """

    def __init__(self, task_to_copy):
        super(copies, self).__init__()
        self.requires_decorator = requires(task_to_copy)

    def __call__(self, task_that_copies):
        task_that_copies = self.requires_decorator(task_that_copies)

        # Modify task_that_copies by subclassing it and adding methods
        @task_wraps(task_that_copies)
        class Wrapped(task_that_copies):

            def run(_self):
                i, o = _self.input(), _self.output()
                f = o.open('w')  # TODO: assert that i, o are Target objects and not complex datastructures
                for line in i.open('r'):
                    f.write(line)
                f.close()

        return Wrapped


def delegates(task_that_delegates):
    """ Lets a task call methods on subtask(s).

    The way this works is that the subtask is run as a part of the task, but
    the task itself doesn't have to care about the requirements of the subtasks.
    The subtask doesn't exist from the scheduler's point of view, and
    its dependencies are instead required by the main task.

    Example:

    .. code-block:: python

        class PowersOfN(luigi.Task):
            n = luigi.IntParameter()
            def f(self, x): return x ** self.n

        @delegates
        class T(luigi.Task):
            def subtasks(self): return PowersOfN(5)
            def run(self): print self.subtasks().f(42)
    """
    if not hasattr(task_that_delegates, 'subtasks'):
        # This method can (optionally) define a couple of delegate tasks that
        # will be accessible as interfaces, meaning that the task can access
        # those tasks and run methods defined on them, etc
        raise AttributeError('%s needs to implement the method "subtasks"' % task_that_delegates)

    @task_wraps(task_that_delegates)
    class Wrapped(task_that_delegates):

        def deps(self):
            # Overrides method in base class
            return task.flatten(self.requires()) + task.flatten([t.deps() for t in task.flatten(self.subtasks())])

        def run(self):
            for t in task.flatten(self.subtasks()):
                t.run()
            task_that_delegates.run(self)

    return Wrapped


def previous(task):
    """
    Return a previous Task of the same family.

    By default checks if this task family only has one non-global parameter and if
    it is a DateParameter, DateHourParameter or DateIntervalParameter in which case
    it returns with the time decremented by 1 (hour, day or interval)
    """
    params = task.get_params()
    previous_params = {}
    previous_date_params = {}

    for param_name, param_obj in params:
        param_value = getattr(task, param_name)

        if isinstance(param_obj, parameter.DateParameter):
            previous_date_params[param_name] = param_value - datetime.timedelta(days=1)
        elif isinstance(param_obj, parameter.DateMinuteParameter):
            previous_date_params[param_name] = param_value - datetime.timedelta(minutes=1)
        elif isinstance(param_obj, parameter.DateHourParameter):
            previous_date_params[param_name] = param_value - datetime.timedelta(hours=1)
        elif isinstance(param_obj, parameter.DateIntervalParameter):
            previous_date_params[param_name] = param_value.prev()
        else:
            previous_params[param_name] = param_value

    previous_params.update(previous_date_params)

    if len(previous_date_params) == 0:
        raise NotImplementedError("No task parameter - can't determine previous task")
    elif len(previous_date_params) > 1:
        raise NotImplementedError("Too many date-related task parameters - can't determine previous task")
    else:
        return task.clone(**previous_params)


def get_previous_completed(task, max_steps=10):
    prev = task
    for _ in xrange(max_steps):
        prev = previous(prev)
        logger.debug("Checking if %s is complete", prev)
        if prev.complete():
            return prev
    return None
