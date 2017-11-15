#!/usr/local/bin/python2.7
# encoding: utf-8
'''
 -- Converts FAIMS exported XML data into a format that can be ingested by Alveo


@author:     Michael Bauer

'''

import sys
import os

from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter

DEBUG = 1
TESTRUN = 0
PROFILE = 0

program_name = "FAIMS to Alveo Converter"

class CLIError(Exception):
    '''Generic exception to raise and log different fatal errors.'''
    def __init__(self, msg):
        super(CLIError).__init__(type(self))
        self.msg = "E: %s" % msg
    def __str__(self):
        return self.msg
    def __unicode__(self):
        return self.msg


def process_data(input=None,output=None,verbose=False):
    #Do stuff here
    
    return 0

def main(argv=None): # IGNORE:C0111
    '''Command line options.'''

    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)

    try:
        # Setup argument parser
        parser = ArgumentParser(formatter_class=RawDescriptionHelpFormatter)
        parser.add_argument("-r", "--recursive", dest="recurse", action="store_true", help="recurse into subfolders [default: %(default)s]")
        parser.add_argument("-v", "--verbose", dest="verbose", action="count", help="set verbosity level [default: %(default)s]")
        parser.add_argument("-i", "--input", dest="input", help="Root Directory of FAIMS data in XML format", metavar="path" )
        parser.add_argument("-o", "--output", dest="output", help="Directory for the resulting files to be placed", metavar="path")
        
        # Process arguments
        args = parser.parse_args()

        if args.verbose > 0:
            verbose = True
        else:
            verbose = False
        
        return process_data(args.input, args.output, verbose)
    except KeyboardInterrupt:
        ### handle keyboard interrupt ###
        return 0
    except Exception, e:
        if DEBUG or TESTRUN:
            raise(e)
        indent = len(program_name) * " "
        sys.stderr.write(program_name + ": " + repr(e) + "\n")
        sys.stderr.write(indent + "  for help use --help")
        return 2

if __name__ == "__main__":
    if DEBUG:
        sys.argv.append("-h")
        sys.argv.append("-v")
        sys.argv.append("-r")
    if TESTRUN:
        import doctest
        doctest.testmod()
    if PROFILE:
        import cProfile
        import pstats
        profile_filename = '_profile.txt'
        cProfile.run('main()', profile_filename)
        statsfile = open("profile_stats.txt", "wb")
        p = pstats.Stats(profile_filename, stream=statsfile)
        stats = p.strip_dirs().sort_stats('cumulative')
        stats.print_stats()
        statsfile.close()
        sys.exit(0)
    sys.exit(main())