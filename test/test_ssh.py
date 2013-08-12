# Copyright (c) 2012 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

from luigi.contrib.ssh import RemoteContext, RemoteTarget
import unittest
import subprocess
import socket

working_ssh_host = None  # set this to a working ssh host string (e.g. "localhost") to activate integration tests


def integration(cls):
    """ Decorator that removes tests if `working_ssh_host` is not set """
    if working_ssh_host is not None:
        return cls


class TestMockedRemoteContext(unittest.TestCase):
    def test_subprocess_delegation(self):
        """ Test subprocess call structure using mock module """
        orig_Popen = subprocess.Popen
        self.last_test = None

        def Popen(cmd, **kwargs):
            self.last_test = cmd

        subprocess.Popen = Popen
        context = RemoteContext(
            "some_host",
            username="luigi",
            key_file="/some/key.pub"
        )
        context.Popen(["ls"])
        self.assertTrue("ssh" in self.last_test)
        self.assertTrue("-i" in self.last_test)
        self.assertTrue("/some/key.pub" in self.last_test)
        self.assertTrue("luigi@some_host" in self.last_test)
        self.assertTrue("ls" in self.last_test)

        subprocess.Popen = orig_Popen

    def test_check_output_fail_connect(self):
        """ Test check_output to a non-existing host """
        context = RemoteContext("__NO_HOST_LIKE_THIS__")
        self.assertRaises(
            subprocess.CalledProcessError,
            context.check_output, ["ls"]
        )


# The following tests require a working ssh server at `working_ssh_host`
# the test runner can ssh into using password-less authentication

# since `nc` has different syntax on different platforms
# we use a short python command to start
# a 'hello'-server on the remote machine
HELLO_SERVER_CMD = """
import socket, sys
listener = socket.socket()
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(('localhost', 2134))
listener.listen(1)
sys.stdout.write('ready')
sys.stdout.flush()
conn = listener.accept()[0]
conn.sendall('hello')
"""


@integration
class TestRemoteContext(unittest.TestCase):
    def setUp(self):
        self.context = RemoteContext(working_ssh_host)

    def test_check_output(self):
        """ Test check_output ssh

        Assumes the running user can ssh to working_ssh_host
        """
        output = self.context.check_output(["echo", "-n", "luigi"])
        self.assertEquals(output, "luigi")

    def test_tunnel(self):
        print "Setting up remote listener..."

        remote_server_handle = self.context.Popen([
            "python", "-c", '"{0}"'.format(HELLO_SERVER_CMD)
        ], stdout=subprocess.PIPE)

        print "Setting up tunnel"
        with self.context.tunnel(2135, 2134):
            print "Tunnel up!"
            # hack to make sure the listener process is up
            # and running before we write to it
            server_output = remote_server_handle.stdout.read(5)
            self.assertEquals(server_output, "ready")
            print "Connecting to server via tunnel"
            s = socket.socket()
            s.connect(("localhost", 2135))
            print "Receiving...",
            response = s.recv(5)
            self.assertEquals(response, "hello")
            print "Closing connection"
            s.close()
            print "Waiting for listener..."
            output, _ = remote_server_handle.communicate()
            self.assertEquals(remote_server_handle.returncode, 0)
            print "Closing tunnel"


@integration
class TestRemoteTarget(unittest.TestCase):
    """ These tests assume RemoteContext working
    in order for setUp and tearDown to work
    """
    def setUp(self):
        self.ctx = RemoteContext(working_ssh_host)
        self.filepath = "/tmp/luigi_remote_test.dat"
        self.target = RemoteTarget(
            self.filepath,
            working_ssh_host,
        )
        self.ctx.check_output(["rm", "-rf", self.filepath])
        self.ctx.check_output(["echo -n 'hello' >", self.filepath])

    def tearDown(self):
        self.ctx.check_output(["rm", "-rf", self.filepath])

    def test_exists(self):
        self.assertTrue(self.target.exists())
        no_file = RemoteTarget(
            "/tmp/_file_that_doesnt_exist_",
            working_ssh_host,
        )
        self.assertFalse(no_file.exists())

    def test_remove(self):
        self.target.remove()
        self.assertRaises(
            subprocess.CalledProcessError,
            self.ctx.check_output,
            ["cat", self.filepath]
        )

    def test_open(self):
        f = self.target.open('r')
        file_content = f.read()
        f.close()
        self.assertEquals(file_content, "hello")

    def test_context_manager(self):
        with self.target.open('r') as f:
            file_content = f.read()

        self.assertEquals(file_content, "hello")
