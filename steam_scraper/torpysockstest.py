import socket
import socks
from urllib.request import urlopen
def main():
    socks.set_default_proxy(socks.SOCKS5,"localhost",9150)
    socket.socket = socks.socksocket
    ipch = 'http://icanhazip.com'
    print(urlopen(ipch).read())


if __name__ =="__main__":
    main()