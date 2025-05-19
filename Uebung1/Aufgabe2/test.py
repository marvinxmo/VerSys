import socket

s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
try:
    # Doesn't need to be reachable
    s.connect(("8.8.8.8", 80))
    local_ip = s.getsockname()[0]
finally:
    s.close()

print(local_ip)
