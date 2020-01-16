import paramiko
import socket
import b9py


def kill_node(host, username, password, pid):
    try:
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(host, username=username, password=password)

        _, ss_stdout, ss_stderr = client.exec_command("kill -9 " + str(pid))
        r_out, r_err = ss_stdout.readlines(), ss_stderr.read()

        if len(r_err) > 0:
            return b9py.B9Status.failed_status(status_text=r_err.decode("utf-8"))

    except paramiko.ssh_exception.AuthenticationException as ex:
        return b9py.B9Status.failed_status(status_text=str(ex))

    except socket.gaierror as ex:
        return b9py.B9Status.failed_status(status_text=str(ex))

    return b9py.B9Status.success_status()


status = kill_node("192.168.3.103", "steve", "", 11116)
print(status)
