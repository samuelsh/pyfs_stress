def undot_ipv4(dotted):
    return sum(int(octet) << ((3 - i) << 3) for i, octet in enumerate(dotted.split('.')))


def dot_ipv4(addr):
    return '.'.join(str(addr >> off & 0xff) for off in (24, 16, 8, 0))


def range_ipv4(start, stop):
    for address in range(undot_ipv4(start), undot_ipv4(stop)):
        yield dot_ipv4(address)


