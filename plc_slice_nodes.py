#!/bin/env python

"""Writes hostname and IPs for all nodes in a PL slice to a file."""

##############################################################################
##############################################################################

import sys, optparse, getpass, socket, threading, Queue

import PLC.Shell

import resolve

##############################################################################
##############################################################################

PLCAPI_URL = 'https://www.planet-lab.org/PLCAPI/'

USAGE = "%prog [options] USER"
DESCRIPTION = """Writes hostname and IP for all nodes in a PL slice to a file.
Required argument USER: PLC user or file containing PLC user.
Prompts for the password if not given on the command line.
Will choose some slice if no slice is specified.
"""

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

def lookup(nodes, interfaces):
    # join nodes to interfaces
    node_to_ips = {}
    missing = []
    for name, n in nodes.iteritems():
        ips = []
        for i in n['interface_ids']:
            ip = interfaces[i]['ip']
            if ip:
                ips.append(ip)
        if ips:
            node_to_ips[name] = ips
        else:
            missing.append(name)
            
    # if IP is not in PLCAPI, do the lookup ourselves
    if len(missing) > 0:
        node_to_ips.update(resolve.resolve(missing))
    return node_to_ips

##############################################################################
##############################################################################

def parse_options(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    parser = optparse.OptionParser(usage=USAGE, description=DESCRIPTION)
    parser.add_option("-u", "--url", help="PLCAPI URL (default: %s)" % PLCAPI_URL, 
                      default=PLCAPI_URL)
    parser.add_option("-p", "--password", 
                      help="PLC password or file containing PLC password (default: prompt)")
    parser.add_option("-s", "--slice", 
                      help="Slice name (default: some slice)")
    parser.add_option("-o", "--output",
                      help="output file (default is stdout)")
        
    opts, args = parser.parse_args(argv)
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
    opts = parse_options(argv)
    shell = connect(opts.user, opts.password, opts.url)
    nodes, interfaces = fetch(shell, opts.slice)
    nodes_to_ips = lookup(nodes, interfaces)
    resolve.output(nodes_to_ips, opts.output)
    
##############################################################################
##############################################################################

if __name__ == '__main__':
    sys.exit(main())
    
##############################################################################
##############################################################################
