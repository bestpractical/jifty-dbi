# $Header: /raid/cvsroot/DBIx/Makefile,v 1.20 2000/09/17 05:28:28 jesse Exp $
# 
#
# Request Tracker is Copyright 1997-9 Jesse Vincent <jesse@fsck.com>
# RT is distributed under the terms of the GNU Public License


VERSION_MAJOR	=	0
VERSION_MINOR	=	0
VERSION_PATCH	=	5

VERSION =	$(VERSION_MAJOR).$(VERSION_MINOR).$(VERSION_PATCH)
TAG 	   =	DBIx-SearchBuilder-$(VERSION_MAJOR)-$(VERSION_MINOR)-$(VERSION_PATCH)

####################################################################
# No user servicable parts below this line.  Frob at your own risk #
####################################################################

install:
	cd DBIx-SearchBuilder; perl Makefile.PL; make install

clean: 
	cd DBIx-SearchBuilder; make clean

default:
	@echo "Read the README"

commit:
	cvs commit

predist: commit
	cvs tag -F $(TAG)
	rm -rf /tmp/$(TAG)
	cvs export -D now -d /tmp/$(TAG) DBIx
	cd /tmp; tar czvf /home/ftp/pub/rt/devel/$(TAG).tar.gz $(TAG)/
	chmod 644 /home/ftp/pub/rt/devel/$(TAG).tar.gz

dist: commit predist
	rm -rf /home/ftp/pub/rt/devel/DBIx-SearchBuilder.tar.gz
	ln -s ./DBIx-SearchBuilder-$(VERSION).tar.gz /home/ftp/pub/rt/devel/JVDBIx.tar.gz
