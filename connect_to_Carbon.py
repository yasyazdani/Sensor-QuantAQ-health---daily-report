# Connecting to Carbon from your local computer using your username and password


#!/usr/bin/env python3
import paramiko
import getpass
import sys
import threading

def interactive_shell(chan):
    def send_input():
        while True:
            data = sys.stdin.read(1)
            if not data:
                break
            chan.send(data)
    threading.Thread(target=send_input, daemon=True).start()

    try:
        while True:
            data = chan.recv(1024)
            if not data:
                break
            sys.stdout.write(data.decode(errors='ignore'))
            sys.stdout.flush()
    except:
        pass

def main():
    host = "carbon.atmosp.physics.utoronto.ca"
    port = 2222
    default_user = "" # write your username here

    user = input(f"Username [{default_user}]: ").strip() or default_user
    password = getpass.getpass(f"Password for {user}@{host}: ")

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=host, port=port, username=user, password=password)

    chan = client.invoke_shell()
    print(f"\n*** Connected to {host} as {user} ***\n")
    interactive_shell(chan)
    chan.close()
    client.close()

if __name__ == "__main__":
    main()

############












