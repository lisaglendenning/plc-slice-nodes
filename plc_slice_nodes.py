#!/bin/env python

"""Writes hostname and IPs for all nodes in a PL slice to a file."""

##############################################################################
##############################################################################

import sys, optparse, getpass, socket, threading, Queue

import PLC.Shell

##############################################################################
##############################################################################

PLCAPI_URL = 'https://www.planet-lab.org/PLCAPI/'

USAGE = "%prog [options] USER"
DESCRIPTION = """Writes hostname and IP for all nodes in a PL slice to a file.
Required argument USER: PLC user or file containing PLC user.
Prompts for the password if not given on the command line.
Will choose some slice if no slice is specified.
"""

OUTPUT_SEP = ' '
OUTPUT_ENDL = '\n'
OUTPUT_COLUMNS = ['%(hostname)s', '%(ip)s']
OUTPUT_RECORD = OUTPUT_SEP.join(OUTPUT_COLUMNS)

##############################################################################
##############################################################################

def connect(user, password, url=PLCAPI_URL):
    """Initializes PLC connection."""
    shell = PLC.Shell.Shell(globals = globals(),
                        url = url, 
                        xmlrpc = True,
                        method = 'password',
                        user = user, 
                        password = password)
    return shell

##############################################################################
##############################################################################

def fetch(shell, slice_name):
    """Returns slice and slice nodes."""
    return_fields = ['name', 'slice_id', 'node_ids',]
    # keyword arguments don't seem to work ?
    slices = GetSlices(slice_name, return_fields)
    if len(slices) < 1:
        raise RuntimeError("No slices found")
    # if slice is not specified, use the first slice returned
    slice = slices[0]

    # sort nodes by hostname
    return_fields = ['node_id', 'hostname', 'interface_ids']
    result = GetNodes(slice['node_ids'], return_fields)
    nodes = {}
    for n in result:
        name = n['hostname']
        assert name not in nodes
        nodes[name] = n
    
    # sort interfaces by id
    interface_ids = []
    for n in nodes.itervalues():
        interface_ids.extend(n['interface_ids'])
    result = GetInterfaces(interface_ids)
    interfaces = {}
    for i in result:
        id = i['interface_id']
        assert id not in interfaces
        interfaces[id] = i
    
    return nodes, interfaces

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
            except Empty:
                break
            else:
                ips = lookup(hostname)
                output.put((hostname, ips))
                input.task_done()

##############################################################################
##############################################################################

# TODO: some sort of timeout enforcement would be nice?
def lookups(nodes, interfaces):
    # join nodes to interfaces
    node_to_ips = {}
    missing = Queue.Queue()
    for name, n in nodes.iteritems():
        ips = []
        for i in n['interface_ids']:
            ip = interfaces[i]['ip']
            if ip:
                ips.append(ip)
        if ips:
            node_to_ips[name] = ips
        else:
            missing.put(name)
            
    # if IP is not in PLCAPI, do the lookup ourselves
    if not missing.empty():
        pool_size = 16
        results = Queue.Queue()
        for i in xrange(pool_size):
            t = LookupThread(missing, results)
            t.start()
        missing.join() # not until Python 2.5
        
        while not results.empty():
            try:
                name, ips = results.get_nowait()
            except Empty:
                break
            else:
                assert name not in node_to_ips
                node_to_ips[name] = ips
    return node_to_ips

##############################################################################
##############################################################################

def output(nodes, interfaces, filename=None):
    """Writes node hostname and IPs to a file (or stdout)."""
    # map nodes to IPs
    node_to_ips = lookups(nodes, interfaces)
    if filename is None:
        file = sys.stdout
    else:
        file = open(filename, 'w')
    names = nodes.keys()
    names.sort()
    for name in names:
        ips = node_to_ips[name]
        record = {'hostname' : name, 'ip': OUTPUT_SEP.join(ips) }
        file.write(OUTPUT_RECORD % record)
        file.write(OUTPUT_ENDL)
    file.close()
    
##############################################################################
##############################################################################

def parse_options(argv):
    parser = optparse.OptionParser(usage=USAGE, description=DESCRIPTION)
    parser.add_option("-u", "--url", help="PLCAPI URL (default: %s)" % PLCAPI_URL, 
                      default=PLCAPI_URL)
    parser.add_option("-p", "--password", 
                      help="PLC password or file containing PLC password (default: prompt)")
    parser.add_option("-s", "--slice", 
                      help="Slice name (default: some slice)")
    parser.add_option("-o", "--output",
                      help="output file (default is stdout)")
        
    opts, args = parser.parse_args()
    if len(args) < 1:
        parser.error("Missing required argument (USER)")
    opts.user = args[0]
    try:
        opts.user = open(opts.user).read().strip()
    except IOError:
        pass
    if opts.password is None:
        try:
            opts.password = getpass.getpass()
        except (EOFError, KeyboardInterrupt):
            return 0
    else:
        try:
            opts.password = open(opts.password).read().strip()
        except IOError:
            pass
    return opts

##############################################################################
##############################################################################

def main(argv=None):

    if argv is None:
        argv = sys.argv
    opts = parse_options(argv)
    
    shell = connect(opts.user, opts.password, opts.url)
    nodes, interfaces = fetch(shell, opts.slice)
    output(nodes, interfaces, opts.output)
    
##############################################################################
##############################################################################

if __name__ == '__main__':
    sys.exit(main(sys.argv))
    
##############################################################################
##############################################################################
