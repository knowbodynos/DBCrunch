#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python

import sys;
from argparse import ArgumentParser;

parser=ArgumentParser();
parser.add_argument('docsfile');
parser.add_argument('--batch','-b',dest='batch',action='store_true',default=False,help='');
parser.add_argument('--basecoll','-c',dest='basecoll',action='store',default=None,help='');
parser.add_argument('--dbindexes','-i',dest='dbindexes',nargs='+',default=None,help='');
parser.add_argument('--write-local','-l',dest='writelocal',action='store_true',default=False,help='');
parser.add_argument('--write-db','-d',dest='writedb',action='store_true',default=False,help='');
parser.add_argument('--stats-local','-L',dest='statslocal',action='store_true',default=False,help='');
parser.add_argument('--stats-db','-D',dest='statsdb',action='store_true',default=False,help='');

def main(parser=parser):
	args=vars(parser.parse_known_args()[0]);
	if any([args[x] for x in ['writelocal','writedb','statslocal','statsdb']]) and ((args[basecoll]==None) or (args[dbindexes]==None)):
	    parser.error("--basecoll and --dbindexes are required for the following options: --write-local, --write-db, --stats-local and --stats-db.");
	print(args);
	sys.stdout.flush();

if __name__=='__main__':
	main();