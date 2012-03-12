#!/bin/env python

"""Resolves IP addresses for a set of domains.

For Python 2.5."""

##############################################################################
##############################################################################

import sys, optparse, socket, threading, Queue

##############################################################################
##############################################################################

USAGE = "%prog [options]"
DESCRIPTION = """Resolves IP addresses for a set of domains."""

COLUMN_SEP = ' '
RECORD_END = '\n'
RECORD_COLUMNS = ['%(hostname)s', '%(ip)s']
RECORD_FORMAT = COLUMN_SEP.join(RECORD_COLUMNS) + RECORD_END

POOL_SIZE = 16 # number of threads

##############################################################################
##############################################################################

def lookup(hostname):
    """Returns list of IPs for a hostname."""
    try:
        result = socket.getaddrinfo(hostname, None)
    except: # Unable to resolve
        return ()
    ips = [r[4][0] for r in result]
    return ips

##############################################################################
##############################################################################

class LookupThread(threading.Thread):
    """Fetches hostnames from input Queue and writes (hostname, IPs) result to output Queue."""
    
    def __init__(self, input, output):
        threading.Thread.__init__(self)
        self.input = input
        self.output = output

    def run(self):
        input = self.input
        output = self.output
        while not input.empty():
            try:
                hostname = input.get(True, 0.1)
            except Queue.Empty:
                break
            else:
                ips = lookup(hostname)
                output.put((hostname, ips))
                input.task_done()

##############################################################################
##############################################################################

# TODO: some sort of timeout enforcement would be nice?
def resolve(hostnames):
    input = Queue.Queue()
    for i in hostnames:
        input.put_nowait(i)
    
    output = Queue.Queue()
    for i in xrange(POOL_SIZE):
        t = LookupThread(input, output)
        t.start()
    input.join() # needs Python 2.5
    
    name_to_ips = {}
    while not output.empty():
        name, ips = output.get_nowait()
        name_to_ips[name] = ips
    return name_to_ips

##############################################################################
##############################################################################

def input(filename=None):
    hostnames = []
    if filename is None:
        f = sys.stdin
    else:
        f = open(filename, 'r')
    for line in f:
        columns = (line.rstrip(RECORD_END)).split(RECORD_SEP)
        hostnames.append(columns[0])
    f.close()
    return hostnames

##############################################################################
##############################################################################

def output(names_to_ips, filename=None):
    """Writes hostname and IPs to a file (or stdout)."""
    if filename is None:
        f = sys.stdout
    else:
        f = open(filename, 'w')
    names = names_to_ips.keys()
    names.sort()
    for name in names:
        ips = names_to_ips[name]
        record = {'hostname' : name, 'ip': COLUMN_SEP.join(ips) }
        f.write(RECORD_FORMAT % record)
    f.close()
    
##############################################################################
##############################################################################

def parse_options(argv=None, values=None):
    if argv is None:
        argv = sys.argv[1:]
    parser = optparse.OptionParser(usage=USAGE, description=DESCRIPTION)
    parser.add_option("-i", "--input",
                      help="input file (default is stdin)")
    parser.add_option("-o", "--output",
                      help="output file (default is stdout)")
        
    opts, args = parser.parse_args(argv, values)
    return opts

##############################################################################
##############################################################################

def main(argv=None):
    opts = parse_options(argv)
    hostnames = input(opts.input)
    names_to_ips = resolve(hostnames)
    output(names_to_ips, opts.output)
    
##############################################################################
##############################################################################

if __name__ == '__main__':
    sys.exit(main())
    
##############################################################################
##############################################################################
